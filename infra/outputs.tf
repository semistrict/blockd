output "bucket" {
  value = google_storage_bucket.store.name
}

output "hosts" {
  value = { for id, host in google_compute_instance.host : id => host.name }
}

output "zone" {
  value = var.zone
}
