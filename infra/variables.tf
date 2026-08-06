# Inputs for the blockd two-host demo. Only `project` has no default.

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

variable "spot" {
  description = "Run the VMs as Spot instances (cheaper, preemptible)"
  type        = bool
  default     = true
}

variable "repo" {
  description = "Git URL the VMs clone and build"
  type        = string
  default     = "https://github.com/semistrict/blockd"
}

variable "repo_ref" {
  description = "Branch or commit to build"
  type        = string
  default     = "main"
}
