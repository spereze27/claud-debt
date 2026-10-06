# =============================================================================
# POC: flujos encadenados (cascadas) y grupos de flujos para el auditor.
#
#   Landing (GCS)                Bronze / Raw (BQ)             Silver / Trusted (BQ)
#   gs://…-landing/ventas/*.csv ─[F1: GCS→BQ, APPEND]─► ventas_raw ─[F2: MERGE]─► ventas ─[F3: vista]─► v_resumen_ventas_region
#   └──────────────────────────── Grupo A · grupo_cascada = ingesta-core · owner = finanzas ────────────────────────────┘
#
#   bigquery-public-data.samples.shakespeare ─[B1: CREATE OR REPLACE]─► kpi_marketing
#   └── Grupo B · grupo_cascada = reporting-aislado · owner = marketing (independiente) ──┘
#
# Cómo puede inferir los grupos el agente (las transferencias de BigQuery NO admiten `labels`):
#   1. Etiquetas de JOB: los scripts ejecutan `SET @@query_label = "grupo_cascada:…,owner:…"`, que aparecen en
#      INFORMATION_SCHEMA.JOBS.labels.
#   2. Convención de nombre en el display_name de la transferencia: "[grupo] nombre_del_flujo".
#   3. Etiquetas de los recursos (bucket, datasets y tablas) que lee o escribe cada flujo.
#   4. Si nada de lo anterior existe: componentes conexos del linaje (grupo "auto-…").
# =============================================================================

terraform {
  required_version = ">= 1.5"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 5.30, < 7.0"
    }
    random = {
      source  = "hashicorp/random"
      version = ">= 3.5"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

data "google_project" "this" {
  project_id = var.project_id
}

resource "random_id" "suffix" {
  byte_length = 3
}

locals {
  suffix = random_id.suffix.hex

  common_labels = {
    poc        = "flujos-encadenados"
    managed_by = "terraform"
  }
  grupo_a = merge(local.common_labels, {
    grupo_cascada = "ingesta-core"
    owner         = "finanzas"
  })
  grupo_b = merge(local.common_labels, {
    grupo_cascada = "reporting-aislado"
    owner         = "marketing"
  })

  bronze_table = "${var.project_id}.${google_bigquery_dataset.bronze.dataset_id}.${google_bigquery_table.ventas_raw.table_id}"
  silver_table = "${var.project_id}.${google_bigquery_dataset.silver.dataset_id}.${google_bigquery_table.ventas.table_id}"
}

# -----------------------------------------------------------------------------
# APIs
# -----------------------------------------------------------------------------
resource "google_project_service" "apis" {
  for_each = toset([
    "bigquery.googleapis.com",
    "bigquerydatatransfer.googleapis.com",
    "storage.googleapis.com",
    "iam.googleapis.com",
  ])
  project            = var.project_id
  service            = each.value
  disable_on_destroy = false
}

# -----------------------------------------------------------------------------
# Capa LANDING: bucket de aterrizaje + archivo de ejemplo
# -----------------------------------------------------------------------------
resource "google_storage_bucket" "landing" {
  name                        = "${var.project_id}-landing-${local.suffix}"
  location                    = var.bq_location
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = true # POC: permite `terraform destroy` con objetos dentro

  labels = merge(local.grupo_a, { capa = "landing" })

  lifecycle_rule {
    condition {
      age = 30
    }
    action {
      type = "Delete"
    }
  }

  depends_on = [google_project_service.apis]
}

resource "google_storage_bucket_object" "ventas_ejemplo" {
  bucket       = google_storage_bucket.landing.name
  name         = "ventas/dt=2026-10-01/ventas_2026_10_01.csv"
  content_type = "text/csv"
  content      = <<-CSV
    venta_id,fecha,region,monto
    V-0001,2026-10-01,andina,150000.50
    V-0002,2026-10-01,caribe,89000.00
    V-0003,2026-10-01,pacifico,230500.75
    V-0004,2026-10-01,andina,45000.00
    V-0002,2026-10-01,caribe,89000.00
  CSV
}

# -----------------------------------------------------------------------------
# Capa BRONZE / RAW
# -----------------------------------------------------------------------------
resource "google_bigquery_dataset" "bronze" {
  dataset_id                 = "bronze_finanzas"
  friendly_name              = "Bronze / Raw - Finanzas"
  description                = "Capa bronze (raw): copia fiel de los archivos de landing, append-only."
  location                   = var.bq_location
  delete_contents_on_destroy = true
  labels                     = merge(local.grupo_a, { capa = "bronze" })
  depends_on                 = [google_project_service.apis]
}

resource "google_bigquery_table" "ventas_raw" {
  dataset_id          = google_bigquery_dataset.bronze.dataset_id
  table_id            = "ventas_raw"
  description         = "Ventas tal como llegan del CSV (todo STRING). Escrita por F1 (append)."
  deletion_protection = false
  labels              = merge(local.grupo_a, { capa = "bronze", flujo_productor = "f1-landing-a-bronze" })

  time_partitioning {
    type = "DAY" # particionada por fecha de ingesta (_PARTITIONTIME)
  }

  schema = jsonencode([
    { name = "venta_id", type = "STRING", mode = "NULLABLE" },
    { name = "fecha", type = "STRING", mode = "NULLABLE" },
    { name = "region", type = "STRING", mode = "NULLABLE" },
    { name = "monto", type = "STRING", mode = "NULLABLE" },
  ])
}

# -----------------------------------------------------------------------------
# Capa SILVER / TRUSTED
# -----------------------------------------------------------------------------
resource "google_bigquery_dataset" "silver" {
  dataset_id                 = "silver_finanzas"
  friendly_name              = "Silver / Trusted - Finanzas"
  description                = "Capa silver (trusted): datos tipados, deduplicados y conformados."
  location                   = var.bq_location
  delete_contents_on_destroy = true
  labels                     = merge(local.common_labels, { capa = "silver", owner = "finanzas" })
  depends_on                 = [google_project_service.apis]
}

resource "google_bigquery_table" "ventas" {
  dataset_id          = google_bigquery_dataset.silver.dataset_id
  table_id            = "ventas"
  description         = "Ventas tipadas y deduplicadas por venta_id. Escrita por F2 (MERGE)."
  deletion_protection = false
  labels              = merge(local.grupo_a, { capa = "silver", flujo_productor = "f2-bronze-a-silver" })
  clustering          = ["region"]

  time_partitioning {
    type  = "DAY"
    field = "fecha"
  }

  schema = jsonencode([
    { name = "venta_id", type = "STRING", mode = "REQUIRED" },
    { name = "fecha", type = "DATE", mode = "NULLABLE" },
    { name = "region", type = "STRING", mode = "NULLABLE" },
    { name = "monto", type = "NUMERIC", mode = "NULLABLE" },
    { name = "_procesado_en", type = "TIMESTAMP", mode = "NULLABLE" },
  ])
}

# F3 (Grupo A): vista lógica de consumo sobre silver. El agente la lee de INFORMATION_SCHEMA.VIEWS.
resource "google_bigquery_table" "v_resumen_ventas_region" {
  dataset_id          = google_bigquery_dataset.silver.dataset_id
  table_id            = "v_resumen_ventas_region"
  description         = "Vista lógica: ventas agregadas por región y día (consumo)."
  deletion_protection = false
  labels              = merge(local.grupo_a, { capa = "silver", flujo = "f3-vista-resumen" })

  view {
    use_legacy_sql = false
    query          = <<-SQL
      SELECT region, fecha, COUNT(*) AS num_ventas, SUM(monto) AS monto_total
      FROM `${local.silver_table}`
      GROUP BY region, fecha
    SQL
  }
}

# -----------------------------------------------------------------------------
# Identidad con la que se ejecutan las transferencias (sólo los permisos mínimos)
# -----------------------------------------------------------------------------
resource "google_service_account" "dts_runner" {
  account_id   = "poc-flujos-dts-${local.suffix}"
  display_name = "POC flujos encadenados - ejecución de transferencias"
  depends_on   = [google_project_service.apis]
}

resource "google_project_iam_member" "runner_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.dts_runner.email}"
}

resource "google_bigquery_dataset_iam_member" "runner_bronze_editor" {
  dataset_id = google_bigquery_dataset.bronze.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.dts_runner.email}"
}

resource "google_bigquery_dataset_iam_member" "runner_silver_editor" {
  dataset_id = google_bigquery_dataset.silver.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.dts_runner.email}"
}

resource "google_storage_bucket_iam_member" "runner_landing_reader" {
  bucket = google_storage_bucket.landing.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.dts_runner.email}"
}

# El agente de servicio de BigQuery Data Transfer debe poder generar tokens de la cuenta de ejecución
# (patrón del ejemplo oficial del proveedor para google_bigquery_data_transfer_config).
resource "google_service_account_iam_member" "dts_agent_token_creator" {
  service_account_id = google_service_account.dts_runner.name
  role               = "roles/iam.serviceAccountTokenCreator"
  member             = "serviceAccount:service-${data.google_project.this.number}@gcp-sa-bigquerydatatransfer.iam.gserviceaccount.com"
  depends_on         = [google_project_service.apis]
}


# -----------------------------------------------------------------------------
# GRUPO A · Cascada de ingesta (ingesta-core)
# -----------------------------------------------------------------------------
# F1: Landing (GCS) → Bronze. Transferencia GCS→BigQuery, APPEND (bronze es append-only).
resource "google_bigquery_data_transfer_config" "f1_landing_a_bronze" {
  display_name           = "[ingesta-core] f1_landing_a_bronze_ventas"
  data_source_id         = "google_cloud_storage"
  location               = var.bq_location
  destination_dataset_id = google_bigquery_dataset.bronze.dataset_id
  schedule               = "every day 02:00"
  service_account_name   = google_service_account.dts_runner.email

  params = {
    data_path_template              = "gs://${google_storage_bucket.landing.name}/ventas/*.csv"
    destination_table_name_template = google_bigquery_table.ventas_raw.table_id
    file_format                     = "CSV"
    write_disposition               = "APPEND"
    skip_leading_rows               = "1"
    max_bad_records                 = "0"
    delete_source_files             = "false"
  }

  depends_on = [
    google_project_iam_member.runner_job_user,
    google_bigquery_dataset_iam_member.runner_bronze_editor,
    google_bigquery_dataset_iam_member.runner_silver_editor,
    google_storage_bucket_iam_member.runner_landing_reader,
    google_service_account_iam_member.dts_agent_token_creator,
    google_storage_bucket_object.ventas_ejemplo,
  ]
}

# F2: Bronze → Silver. Consulta programada (script) con MERGE: tipa, deduplica y hace upsert.
# Una hora después de F1 para respetar el orden de la cascada.
resource "google_bigquery_data_transfer_config" "f2_bronze_a_silver" {
  display_name         = "[ingesta-core] f2_bronze_a_silver_ventas"
  data_source_id       = "scheduled_query"
  location             = var.bq_location
  schedule             = "every day 03:00"
  service_account_name = google_service_account.dts_runner.email

  params = {
    query = <<-SQL
      SET @@query_label = "grupo_cascada:ingesta-core,owner:finanzas,flujo:f2-bronze-a-silver,capa_entrada:bronze,capa_salida:silver";

      MERGE `${local.silver_table}` AS t
      USING (
        SELECT
          venta_id,
          SAFE.PARSE_DATE('%Y-%m-%d', fecha) AS fecha,
          LOWER(TRIM(region))               AS region,
          SAFE_CAST(monto AS NUMERIC)       AS monto
        FROM `${local.bronze_table}`
        WHERE venta_id IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (PARTITION BY venta_id ORDER BY _PARTITIONTIME DESC) = 1
      ) AS s
      ON t.venta_id = s.venta_id
      WHEN MATCHED THEN
        UPDATE SET fecha = s.fecha, region = s.region, monto = s.monto, _procesado_en = CURRENT_TIMESTAMP()
      WHEN NOT MATCHED THEN
        INSERT (venta_id, fecha, region, monto, _procesado_en)
        VALUES (s.venta_id, s.fecha, s.region, s.monto, CURRENT_TIMESTAMP());
    SQL
  }

  depends_on = [
    google_project_iam_member.runner_job_user,
    google_bigquery_dataset_iam_member.runner_bronze_editor,
    google_bigquery_dataset_iam_member.runner_silver_editor,
    google_storage_bucket_iam_member.runner_landing_reader,
    google_service_account_iam_member.dts_agent_token_creator,
  ]
}

# -----------------------------------------------------------------------------
# GRUPO B · Flujo independiente (reporting-aislado)
# -----------------------------------------------------------------------------
# B1: lee un dataset público y recrea su propia tabla. No comparte entradas ni salidas con el Grupo A.
resource "google_bigquery_data_transfer_config" "b1_kpi_marketing" {
  display_name         = "[reporting-aislado] b1_kpi_marketing_independiente"
  data_source_id       = "scheduled_query"
  location             = var.bq_location
  schedule             = "every monday 08:00"
  service_account_name = google_service_account.dts_runner.email

  params = {
    query = <<-SQL
      SET @@query_label = "grupo_cascada:reporting-aislado,owner:marketing,flujo:b1-kpi-marketing";

      CREATE OR REPLACE TABLE `${var.project_id}.${google_bigquery_dataset.silver.dataset_id}.kpi_marketing`
      OPTIONS (labels = [("grupo_cascada", "reporting-aislado"), ("owner", "marketing"), ("capa", "silver")])
      AS
      SELECT corpus, SUM(word_count) AS palabras, COUNT(DISTINCT word) AS palabras_unicas,
             CURRENT_TIMESTAMP() AS calculado_en
      FROM `bigquery-public-data.samples.shakespeare`
      GROUP BY corpus;
    SQL
  }

  depends_on = [
    google_project_iam_member.runner_job_user,
    google_bigquery_dataset_iam_member.runner_bronze_editor,
    google_bigquery_dataset_iam_member.runner_silver_editor,
    google_storage_bucket_iam_member.runner_landing_reader,
    google_service_account_iam_member.dts_agent_token_creator,
  ]
}
