output "landing_bucket" {
  value = google_storage_bucket.landing.name
}

output "datasets" {
  value = {
    bronze = google_bigquery_dataset.bronze.dataset_id
    silver = google_bigquery_dataset.silver.dataset_id
  }
}

output "transfer_configs" {
  description = "Nombres de las transferencias (para lanzarlas a mano o verlas en la consola)."
  value = {
    f1_landing_a_bronze = google_bigquery_data_transfer_config.f1_landing_a_bronze.name
    f2_bronze_a_silver  = google_bigquery_data_transfer_config.f2_bronze_a_silver.name
    b1_kpi_marketing    = google_bigquery_data_transfer_config.b1_kpi_marketing.name
  }
}

output "ejecutar_ahora" {
  description = "Comandos para generar historial en INFORMATION_SCHEMA.JOBS sin esperar al calendario (F1 antes que F2)."
  value = [for name in [
    google_bigquery_data_transfer_config.f1_landing_a_bronze.name,
    google_bigquery_data_transfer_config.f2_bronze_a_silver.name,
    google_bigquery_data_transfer_config.b1_kpi_marketing.name,
  ] : "bq mk --transfer_run --run_time=\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\" ${name}"]
}
