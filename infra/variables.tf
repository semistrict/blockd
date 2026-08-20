# Inputs for the three-host production composition. Only `project` has no default.

variable "project" {
  description = "Existing GCP project id"
  type        = string
}

variable "region" {
  description = "Region for the VMs and the bucket"
  type        = string
  default     = "us-central1"
}

variable "zone" {
  description = "Zone for the VMs"
  type        = string
  default     = "us-central1-a"
}

variable "machine_type" {
  description = "Intel machine type (nested virtualization requires Intel)"
  type        = string
  default     = "n2-standard-4"
}

variable "data_disk_size_gb" {
  description = "Size of each host's dedicated XFS blob volume"
  type        = number
  default     = 50

  validation {
    condition     = var.data_disk_size_gb >= 10
    error_message = "data_disk_size_gb must be at least 10 GB."
  }
}

variable "object_prefix" {
  description = "Optional bucket-relative cluster prefix used by both daemon URIs and IAM conditions"
  type        = string
  default     = ""

  validation {
    condition = var.object_prefix == "" || can(regex(
      "^[A-Za-z0-9][A-Za-z0-9._-]*(/[A-Za-z0-9][A-Za-z0-9._-]*)*/?$",
      var.object_prefix,
    ))
    error_message = "object_prefix must contain only alphanumeric-led components using letters, digits, dot, underscore, or hyphen."
  }
}

variable "spot" {
  description = "Run the VMs as Spot instances (explicitly opt in to preemption)"
  type        = bool
  default     = false
}

variable "repo" {
  description = "Git URL the VMs clone and build"
  type        = string
  default     = "https://github.com/semistrict/blockd"
}

variable "repo_ref" {
  description = "Immutable full Git commit ID to build"
  type        = string

  validation {
    condition     = can(regex("^[0-9a-fA-F]{40}$", var.repo_ref))
    error_message = "repo_ref must be a full 40-hex commit ID."
  }
}

variable "base_image" {
  description = "Immutable GCE Ubuntu image resource, never an image family"
  type        = string
  default     = "projects/ubuntu-os-cloud/global/images/ubuntu-2404-noble-amd64-v20260807"

  validation {
    condition = can(regex(
      "^projects/ubuntu-os-cloud/global/images/ubuntu-2404-noble-amd64-v[0-9]{8}$",
      var.base_image,
    ))
    error_message = "base_image must name one immutable dated Ubuntu 24.04 image resource."
  }
}
