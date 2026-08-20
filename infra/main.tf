# blockd production composition on GCP: three nested-virt Intel storage VMs,
# one independently credentialed archive-collector VM, one GCS bucket, and a
# VPC where the storage peer port never leaves the private network. The demo
# application remains a separate composition under `demo/`.

terraform {
  required_version = "= 1.12.5"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "= 6.50.0"
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
    2 = "10.10.0.12"
  }
  object_prefix      = trim(var.object_prefix, "/")
  object_prefix_path = local.object_prefix == "" ? "" : "${local.object_prefix}/"
}

# ── network ─────────────────────────────────────────────────────────────

resource "google_compute_network" "demo" {
  name                    = "blockd"
  auto_create_subnetworks = false
}

resource "google_compute_subnetwork" "demo" {
  name          = "blockd"
  network       = google_compute_network.demo.id
  ip_cidr_range = "10.10.0.0/24"
  region        = var.region
}

# SSH via IAP only — the VMs have no public ingress beyond this.
resource "google_compute_firewall" "iap_ssh" {
  name          = "blockd-iap-ssh"
  network       = google_compute_network.demo.name
  source_ranges = ["35.235.240.0/20"]
  target_tags   = ["blockd-host"]
  allow {
    protocol = "tcp"
    ports    = ["22"]
  }
}

# The peer protocol stays strictly host-to-host inside the VPC.
resource "google_compute_firewall" "internal" {
  name        = "blockd-internal"
  network     = google_compute_network.demo.name
  source_tags = ["blockd-host"]
  target_tags = ["blockd-host"]
  allow {
    protocol = "tcp"
    ports    = ["7001"]
  }
  allow {
    protocol = "icmp"
  }
}

# ── store ───────────────────────────────────────────────────────────────

resource "google_storage_bucket" "store" {
  name                        = "${var.project}-blockd"
  location                    = var.region
  uniform_bucket_level_access = true
  force_destroy               = false
}

resource "google_service_account" "demo" {
  account_id   = "blockd-host"
  display_name = "blockd storage hosts"
}

# Runtime hosts may read the bucket and create or conditionally replace
# records, but do not receive an unqualified bucket-wide DELETE permission.
resource "google_project_iam_custom_role" "host_writer" {
  role_id     = "blockdHostWriter"
  title       = "blockd host object writer"
  description = "Create and conditionally update blockd objects without bucket-wide deletion"
  permissions = [
    "storage.objects.create",
    "storage.objects.update",
  ]
}

resource "google_storage_bucket_iam_member" "host_reader" {
  bucket = google_storage_bucket.store.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.demo.email}"
}

resource "google_storage_bucket_iam_member" "host_writer" {
  bucket = google_storage_bucket.store.name
  role   = google_project_iam_custom_role.host_writer.name
  member = "serviceAccount:${google_service_account.demo.email}"
}

# Hosts delete only normal archive data and their membership leases. Permanent
# HostId claims are deliberately excluded from every deletion role.
resource "google_project_iam_custom_role" "host_delete" {
  role_id     = "blockdHostDelete"
  title       = "blockd host scoped object deleter"
  description = "Delete archive objects and the host's generation-conditional membership lease"
  permissions = ["storage.objects.delete"]
}

resource "google_storage_bucket_iam_member" "host_archive_delete" {
  bucket = google_storage_bucket.store.name
  role   = google_project_iam_custom_role.host_delete.name
  member = "serviceAccount:${google_service_account.demo.email}"

  condition {
    title       = "archive-and-membership-only"
    description = "Limit host deletes to archive objects and membership leases"
    expression = join(" || ", [
      "resource.name.startsWith('projects/_/buckets/${google_storage_bucket.store.name}/objects/${local.object_prefix_path}v/')",
      "resource.name.startsWith('projects/_/buckets/${google_storage_bucket.store.name}/objects/${local.object_prefix_path}b/')",
      "resource.name.startsWith('projects/_/buckets/${google_storage_bucket.store.name}/objects/${local.object_prefix_path}cluster/tls/public-keys/')",
    ])
  }
}

resource "google_service_account" "collector" {
  account_id   = "blockd-collector"
  display_name = "blockd archive collector"
}

resource "google_project_iam_custom_role" "collector_list_role" {
  role_id     = "blockdCollectorList"
  title       = "blockd collector object lister"
  description = "List object names so the collector can enumerate archive prefixes"
  permissions = [
    "storage.objects.list",
  ]
}

resource "google_project_iam_custom_role" "collector_gc_role" {
  role_id     = "blockdCollectorGc"
  title       = "blockd collector archive reader and deleter"
  description = "Read and conditionally delete archive objects without mutation privileges"
  permissions = [
    "storage.objects.delete",
    "storage.objects.get",
  ]
}

# Object listing is authorized at bucket scope. Object bodies and deletion are
# separately restricted to the two archive namespaces below.
resource "google_storage_bucket_iam_member" "collector_list" {
  bucket = google_storage_bucket.store.name
  role   = google_project_iam_custom_role.collector_list_role.name
  member = "serviceAccount:${google_service_account.collector.email}"
}

resource "google_storage_bucket_iam_member" "collector_archive_gc" {
  bucket = google_storage_bucket.store.name
  role   = google_project_iam_custom_role.collector_gc_role.name
  member = "serviceAccount:${google_service_account.collector.email}"

  condition {
    title       = "archive-only"
    description = "Collector can read and delete only recognized archive namespaces"
    expression = join(" || ", [
      "resource.name.startsWith('projects/_/buckets/${google_storage_bucket.store.name}/objects/${local.object_prefix_path}v/')",
      "resource.name.startsWith('projects/_/buckets/${google_storage_bucket.store.name}/objects/${local.object_prefix_path}b/')",
    ])
  }
}

# ── hosts ───────────────────────────────────────────────────────────────

# GCS credentials come from the VM's ambient default service account. Keep the
# collector on its own workload so the collector IAM grant is a real boundary,
# not merely a different Unix user on a storage host.
resource "google_compute_instance" "collector" {
  name         = "blockd-collector"
  machine_type = "e2-small"
  tags         = ["blockd-collector"]

  boot_disk {
    initialize_params {
      image = var.base_image
      size  = 20
      type  = "pd-balanced"
    }
  }

  network_interface {
    subnetwork = google_compute_subnetwork.demo.id
    access_config {}
  }

  service_account {
    email  = google_service_account.collector.email
    scopes = ["cloud-platform"]
  }

  metadata = {
    blockd-bucket   = google_storage_bucket.store.name
    blockd-prefix   = local.object_prefix_path
    blockd-repo     = var.repo
    blockd-repo-ref = var.repo_ref
    blockd-deployment-id = sha256(join("\n", [
      var.repo_ref,
      filesha256("${path.module}/provision-collector.sh"),
      filesha256("${path.module}/blockd-gc.service"),
      var.base_image,
      google_storage_bucket.store.name,
      local.object_prefix_path,
    ]))
  }

  metadata_startup_script = file("${path.module}/provision-collector.sh")

  depends_on = [
    google_storage_bucket_iam_member.collector_list,
    google_storage_bucket_iam_member.collector_archive_gc,
  ]
}

# Keep daemon blobs off the boot filesystem. Each host gets its own durable
# SSD volume; provisioning formats it as XFS only when it is blank and mounts
# it at /var/opt/blockd/blobs.
resource "google_compute_disk" "data" {
  for_each = local.hosts
  name     = "blockd-${each.key}-data"
  type     = "pd-ssd"
  size     = var.data_disk_size_gb
}

resource "google_compute_instance" "host" {
  for_each     = local.hosts
  name         = "blockd-${each.key}"
  machine_type = var.machine_type
  tags         = ["blockd-host"]

  boot_disk {
    initialize_params {
      image = var.base_image
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
    blockd-peer-ip  = each.value
    blockd-bucket   = google_storage_bucket.store.name
    blockd-prefix   = local.object_prefix_path
    blockd-repo     = var.repo
    blockd-repo-ref = var.repo_ref
    blockd-deployment-id = sha256(join("\n", [
      var.repo_ref,
      filesha256("${path.module}/provision.sh"),
      filesha256("${path.module}/blockd.service"),
      var.base_image,
      google_storage_bucket.store.name,
      local.object_prefix_path,
      each.value,
      tostring(var.data_disk_size_gb),
    ]))
  }

  metadata_startup_script = file("${path.module}/provision.sh")
}
