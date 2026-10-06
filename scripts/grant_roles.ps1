# Otorga a la cuenta de servicio del auditor los roles de SÓLO LECTURA necesarios.
# Uso: .\scripts\grant_roles.ps1 -ProjectId <ID> -KeyFile .\credenciales.json [-ParentId <ID> -ParentType folder|organization]
param([Parameter(Mandatory)][string]$ProjectId, [Parameter(Mandatory)][string]$KeyFile,
      [string]$ParentId = "", [ValidateSet("folder", "organization")][string]$ParentType = "folder")
$ErrorActionPreference = "Stop"
$SaEmail = (Get-Content $KeyFile | ConvertFrom-Json).client_email
Write-Host "Cuenta de servicio: $SaEmail"
$Roles = @("roles/cloudasset.viewer", "roles/iam.securityReviewer", "roles/compute.viewer", "roles/monitoring.viewer",
           "roles/bigquery.metadataViewer", "roles/bigquery.resourceViewer", "roles/bigquery.jobUser", "roles/bigquery.user",
           "roles/dataflow.viewer", "roles/dataproc.viewer", "roles/composer.user",
           "roles/composer.environmentAndStorageObjectViewer", "roles/storage.objectViewer", "roles/datastream.viewer")
foreach ($Role in $Roles) {
  if ($ParentId) {
    gcloud resource-manager "$($ParentType)s" add-iam-policy-binding $ParentId --member="serviceAccount:$SaEmail" --role=$Role --condition=None --quiet | Out-Null
  } else {
    gcloud projects add-iam-policy-binding $ProjectId --member="serviceAccount:$SaEmail" --role=$Role --condition=None --quiet | Out-Null
  }
  Write-Host "  OK $Role"
}
if ($ParentId) {
  gcloud resource-manager "$($ParentType)s" add-iam-policy-binding $ParentId --member="serviceAccount:$SaEmail" --role=roles/browser --condition=None --quiet | Out-Null
  Write-Host "  OK roles/browser"
}
$Billing = (gcloud billing projects describe $ProjectId --format="value(billingAccountName)") -replace "billingAccounts/", ""
if ($Billing) {
  try { gcloud billing accounts add-iam-policy-binding $Billing --member="serviceAccount:$SaEmail" --role=roles/billing.viewer | Out-Null
        Write-Host "  OK roles/billing.viewer en $Billing" }
  catch { Write-Host "  AVISO: no se pudo asignar roles/billing.viewer (requiere Billing Account Administrator)" }
}
gcloud services enable cloudasset.googleapis.com cloudresourcemanager.googleapis.com compute.googleapis.com `
  iam.googleapis.com monitoring.googleapis.com cloudbilling.googleapis.com billingbudgets.googleapis.com `
  bigquery.googleapis.com bigquerydatatransfer.googleapis.com --project $ProjectId
Write-Host "Listo. Los permisos pueden tardar 1-2 minutos en propagarse."
