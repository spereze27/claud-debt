variable "project_id" {
  description = "ID del proyecto de GCP donde se despliega la POC."
  type        = string
}

variable "region" {
  description = "Región por defecto del proveedor."
  type        = string
  default     = "us-central1"
}

variable "bq_location" {
  description = "Ubicación de datasets, bucket y transferencias. Debe ser US para que el Grupo B lea bigquery-public-data.samples."
  type        = string
  default     = "US"
}
