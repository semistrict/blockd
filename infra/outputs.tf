output "bucket" {
  value = google_storage_bucket.store.name
}

output "collector_service_account" {
  value       = google_service_account.collector.email
  description = "Dedicated identity for the separately operated archive collector"
}

output "collector" {
  value       = google_compute_instance.collector.name
  description = "Separately deployed archive collector workload"
}

output "hosts" {
  value = { for id, host in google_compute_instance.host : id => host.name }
}

output "zone" {
  value = var.zone
}
