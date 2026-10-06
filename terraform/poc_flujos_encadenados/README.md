# POC: flujos encadenados y grupos de flujos

Despliega una cadena real en BigQuery + Cloud Storage para validar que el auditor reconstruye el linaje,
las capas y los grupos de cascada.

| Flujo | Grupo (`grupo_cascada`) | Motor | Entrada → Salida | Modo |
|---|---|---|---|---|
| F1 `f1_landing_a_bronze_ventas` | `ingesta-core` | Transferencia GCS→BigQuery | `gs://…-landing/ventas/*.csv` (landing) → `bronze_finanzas.ventas_raw` | append |
| F2 `f2_bronze_a_silver_ventas` | `ingesta-core` | Consulta programada (script) | `bronze_finanzas.ventas_raw` → `silver_finanzas.ventas` | merge |
| F3 `v_resumen_ventas_region` | `ingesta-core` | Vista lógica | `silver_finanzas.ventas` → vista de consumo | vista |
| B1 `b1_kpi_marketing_independiente` | `reporting-aislado` | Consulta programada (script) | `bigquery-public-data.samples.shakespeare` → `silver_finanzas.kpi_marketing` | sobrescribe |

Las transferencias de BigQuery **no admiten `labels`**. Por eso el grupo se declara de tres formas, y el
agente usa la primera que encuentra:
1. `SET @@query_label = "grupo_cascada:…,owner:…"` dentro del script: llega a `INFORMATION_SCHEMA.JOBS.labels`.
2. El prefijo `[grupo]` en el `display_name` de la transferencia (F1).
3. Las etiquetas del bucket, los datasets y las tablas que el flujo lee o escribe.

## Uso
```bash
cd terraform/poc_flujos_encadenados
cp terraform.tfvars.example terraform.tfvars      # pon tu project_id
terraform init && terraform apply

# Lanza los flujos ya (en orden) para tener historial en INFORMATION_SCHEMA.JOBS:
terraform output -json ejecutar_ahora | jq -r '.[]' | while read -r cmd; do eval "$cmd"; sleep 60; done
```
Después, ejecuta el auditor sobre el proyecto y descarga el **Excel de linaje** desde la interfaz.

Notas:
- `bq_location` debe ser `US` para que B1 pueda leer `bigquery-public-data.samples`.
- Si tu organización restringe la creación de cuentas de servicio o los dominios en IAM, ajusta `dts_runner`.
- `terraform destroy` lo elimina todo (el bucket y los datasets tienen borrado forzado por ser una POC).
