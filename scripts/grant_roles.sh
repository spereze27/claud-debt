#!/usr/bin/env bash
# Otorga a la cuenta de servicio del auditor los roles de SÓLO LECTURA necesarios.
# Uso: ./scripts/grant_roles.sh <PROJECT_ID> <credenciales.json> [FOLDER_ID|ORG_ID] [folder|organization]
set -euo pipefail
PROJECT_ID="$1"; KEY_FILE="$2"; PARENT_ID="${3:-}"; PARENT_TYPE="${4:-folder}"
SA_EMAIL=$(python -c "import json,sys;print(json.load(open(sys.argv[1]))['client_email'])" "$KEY_FILE")
echo "Cuenta de servicio: $SA_EMAIL"

ROLES=(
  roles/cloudasset.viewer              # arquitectura completa
  roles/iam.securityReviewer           # políticas IAM y claves
  roles/compute.viewer                 # VMs, discos, firewalls
  roles/monitoring.viewer              # uso de CPU/memoria, tráfico
  roles/bigquery.metadataViewer        # datasets, tablas, esquemas
  roles/bigquery.resourceViewer        # historial de jobs (JOBS_BY_PROJECT)
  roles/bigquery.jobUser               # ejecutar consultas a INFORMATION_SCHEMA
  roles/bigquery.user                  # consultas programadas (Data Transfer)
  roles/dataflow.viewer
  roles/dataproc.viewer
  roles/composer.user                  # entornos de Composer
  roles/composer.environmentAndStorageObjectViewer   # leer los DAGs
  roles/storage.objectViewer           # muestrear el data lake
  roles/datastream.viewer
)
for ROLE in "${ROLES[@]}"; do
  if [[ -n "$PARENT_ID" ]]; then
    gcloud resource-manager "${PARENT_TYPE}s" add-iam-policy-binding "$PARENT_ID" \
      --member="serviceAccount:$SA_EMAIL" --role="$ROLE" --condition=None --quiet >/dev/null
  else
    gcloud projects add-iam-policy-binding "$PROJECT_ID" \
      --member="serviceAccount:$SA_EMAIL" --role="$ROLE" --condition=None --quiet >/dev/null
  fi
  echo "  ✔ $ROLE"
done
if [[ -n "$PARENT_ID" ]]; then
  gcloud resource-manager "${PARENT_TYPE}s" add-iam-policy-binding "$PARENT_ID" \
    --member="serviceAccount:$SA_EMAIL" --role=roles/browser --condition=None --quiet >/dev/null
  echo "  ✔ roles/browser (listar proyectos de la carpeta/organización)"
fi

BILLING=$(gcloud billing projects describe "$PROJECT_ID" --format="value(billingAccountName)" | sed 's#billingAccounts/##')
if [[ -n "$BILLING" ]]; then
  gcloud billing accounts add-iam-policy-binding "$BILLING" --member="serviceAccount:$SA_EMAIL" \
    --role=roles/billing.viewer >/dev/null && echo "  ✔ roles/billing.viewer en $BILLING" \
    || echo "  ⚠ No se pudo asignar roles/billing.viewer (requiere Billing Account Administrator)"
fi

gcloud services enable cloudasset.googleapis.com cloudresourcemanager.googleapis.com compute.googleapis.com \
  iam.googleapis.com monitoring.googleapis.com cloudbilling.googleapis.com billingbudgets.googleapis.com \
  bigquery.googleapis.com bigquerydatatransfer.googleapis.com --project "$PROJECT_ID"
echo "Listo. Los permisos pueden tardar 1-2 minutos en propagarse."
