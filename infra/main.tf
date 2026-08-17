# blockd two-host demo on GCP: two nested-virt Intel VMs running demod
# (the blockd daemon + Firecracker + the demo API), one GCS bucket as the
# object store, and a VPC where the peer and API ports never leave the
# private network. `tofu destroy` removes everything, bucket included.

terraform {
  required_version = ">= 1.6"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.0"
    }
  }
}

provider "google" {
  project = var.project
  region  = var.region
  zone    = var.zone
}

locals {
  hosts = {
    0 = "10.10.0.10"
    1 = "10.10.0.11"
  }
}

# ── network ─────────────────────────────────────────────────────────────

resource "google_compute_network" "demo" {
  name                    = "blockd-demo"
  auto_create_subnetworks = false
}

resource "google_compute_subnetwork" "demo" {
  name          = "blockd-demo"
  network       = google_compute_network.demo.id
  ip_cidr_range = "10.10.0.0/24"
  region        = var.region
}

# SSH via IAP only — the VMs have no public ingress beyond this.
resource "google_compute_firewall" "iap_ssh" {
  name          = "blockd-demo-iap-ssh"
  network       = google_compute_network.demo.name
  source_ranges = ["35.235.240.0/20"]
  target_tags   = ["blockd-demo"]
  allow {
    protocol = "tcp"
    ports    = ["22"]
  }
}

# Peer protocol + demo API stay strictly host-to-host inside the VPC.
resource "google_compute_firewall" "internal" {
  name        = "blockd-demo-internal"
  network     = google_compute_network.demo.name
  source_tags = ["blockd-demo"]
  target_tags = ["blockd-demo"]
  allow {
    protocol = "tcp"
    ports    = ["7000", "7001"]
  }
  allow {
    protocol = "icmp"
  }
}

# ── store ───────────────────────────────────────────────────────────────

resource "google_storage_bucket" "store" {
  name                        = "${var.project}-blockd-demo"
  location                    = var.region
  uniform_bucket_level_access = true
  force_destroy               = true
}

resource "google_service_account" "demo" {
  account_id   = "blockd-demo"
  display_name = "blockd demo VMs"
}

# The VMs can touch exactly this bucket, nothing else.
resource "google_storage_bucket_iam_member" "store" {
  bucket = google_storage_bucket.store.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.demo.email}"
}

# ── hosts ───────────────────────────────────────────────────────────────

data "google_compute_image" "ubuntu" {
  family  = "ubuntu-2404-lts-amd64"
  project = "ubuntu-os-cloud"
}

# Keep daemon blobs off the boot filesystem. Each host gets its own durable
# SSD volume; provisioning formats it as XFS only when it is blank and mounts
# it at /var/opt/blockd/blobs.
resource "google_compute_disk" "data" {
  for_each = local.hosts
  name     = "blockd-demo-${each.key}-data"
  type     = "pd-ssd"
  size     = var.data_disk_size_gb
}

resource "google_compute_instance" "host" {
  for_each     = local.hosts
  name         = "blockd-demo-${each.key}"
  machine_type = var.machine_type
  tags         = ["blockd-demo"]

  boot_disk {
    initialize_params {
      image = data.google_compute_image.ubuntu.self_link
      size  = 50
      type  = "pd-ssd"
    }
  }

  attached_disk {
    source      = google_compute_disk.data[each.key].id
    device_name = "blockd-data"
    mode        = "READ_WRITE"
  }

  # Firecracker needs /dev/kvm: nested virtualization, Intel-only.
  advanced_machine_features {
    enable_nested_virtualization = true
  }

  scheduling {
    provisioning_model          = var.spot ? "SPOT" : "STANDARD"
    preemptible                 = var.spot
    automatic_restart           = !var.spot
    instance_termination_action = var.spot ? "STOP" : null
  }

  network_interface {
    subnetwork = google_compute_subnetwork.demo.id
    network_ip = each.value
    # Ephemeral public IP for egress (apt, rustup, github); no inbound
    # rules point at it.
    access_config {}
  }

  service_account {
    email  = google_service_account.demo.email
    scopes = ["cloud-platform"]
  }

  metadata = {
    blockd-host-id  = each.key
    blockd-peer-ip  = each.value
    blockd-bucket   = google_storage_bucket.store.name
    blockd-repo     = var.repo
    blockd-repo-ref = var.repo_ref
  }

  metadata_startup_script = file("${path.module}/provision.sh")
}
