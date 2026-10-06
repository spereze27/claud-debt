"""Íconos oficiales de GCP (y de sistemas on-premise) para los diagramas.

Se toman del paquete `diagrams` (licencia MIT), que incluye los íconos de arquitectura
de Google Cloud, sin necesidad de instalar Graphviz. Se incrustan como data-URI para
que el diagrama interactivo funcione sin servir archivos.
"""
from __future__ import annotations

import base64
import importlib.util
import os
from functools import lru_cache

ICON_FILES = {
    # Cómputo y serverless
    "Cloud Run": "gcp/compute/run.png", "Cloud Run Job": "gcp/compute/run.png",
    "Cloud Functions": "gcp/compute/functions.png", "Compute Engine": "gcp/compute/compute-engine.png",
    "GKE": "gcp/compute/kubernetes-engine.png", "App Engine": "gcp/compute/app-engine.png",
    # Datos y analítica
    "BigQuery": "gcp/analytics/bigquery.png", "Pub/Sub Topic": "gcp/analytics/pubsub.png",
    "Pub/Sub Subscription": "gcp/analytics/pubsub.png", "Dataflow": "gcp/analytics/dataflow.png",
    "Dataproc": "gcp/analytics/dataproc.png", "Composer": "gcp/analytics/composer.png",
    "Data Fusion": "gcp/analytics/data-fusion.png", "Looker": "gcp/analytics/looker.png",
    "Data Transfer": "gcp/analytics/bigquery.png", "Dataform": "gcp/analytics/bigquery.png",
    "Datastream": "gcp/migration/transfer-appliance.png", "Dataplex": "gcp/analytics/data-catalog.png",
    # Bases de datos y almacenamiento
    "Cloud SQL": "gcp/database/sql.png", "Firestore": "gcp/database/firestore.png",
    "Memorystore": "gcp/database/memorystore.png", "Bigtable": "gcp/database/bigtable.png",
    "Spanner": "gcp/database/spanner.png", "Cloud Storage": "gcp/storage/storage.png",
    "Filestore": "gcp/storage/filestore.png",
    # Orquestación, red, seguridad
    "Cloud Scheduler": "gcp/devtools/scheduler.png", "Cloud Tasks": "gcp/devtools/tasks.png",
    "Workflows": "gcp/devtools/tasks.png", "Eventarc": "gcp/api/endpoints.png", "Cloud Build": "gcp/devtools/build.png",
    "Artifact Registry": "gcp/devtools/container-registry.png", "VPC Connector": "gcp/network/virtual-private-cloud.png",
    "Secret Manager": "gcp/security/secret-manager.png", "Cloud KMS": "gcp/security/key-management-service.png",
    "Service Account": "gcp/security/iam.png", "Vertex AI": "gcp/ml/vertex-ai.png",
    "API habilitada": "gcp/api/api-gateway.png", "Monitoring": "gcp/operations/monitoring.png",
    # Externos / on-premise
    "Internet": "onprem/network/internet.png", "Endpoint externo": "onprem/network/internet.png",
    "Usuario/Grupo": "onprem/client/users.png", "postgres": "onprem/database/postgresql.png",
    "mysql": "onprem/database/mysql.png", "oracle": "onprem/database/oracle.png", "mongodb": "onprem/database/mongodb.png",
    "hdfs": "onprem/analytics/hadoop.png", "hive": "onprem/analytics/hive.png", "spark": "onprem/analytics/spark.png",
    "kafka": "onprem/queue/kafka.png", "sftp": "onprem/compute/server.png", "http": "onprem/network/internet.png",
    "ext": "onprem/compute/server.png", "cloudsql": "gcp/database/sql.png",
}

# Motor del flujo → ícono (para el diagrama de linaje)
FLOW_ICON = {"bq_scheduled_query": "BigQuery", "bq_query": "BigQuery", "bq_load": "BigQuery", "dts": "Data Transfer",
             "dataform": "Dataform", "dbt": "BigQuery", "dataflow": "Dataflow", "dataproc": "Dataproc",
             "composer_dag": "Composer", "datastream": "Datastream", "microservice": "Cloud Run", "catalog": "ext",
             "bq_view": "BigQuery"}
SYSTEM_ICON = {"bigquery": "BigQuery", "gcs": "Cloud Storage", "pubsub": "Pub/Sub Topic", "postgres": "postgres",
               "postgresql": "postgres", "mysql": "mysql", "oracle": "oracle", "hdfs": "hdfs", "kafka": "kafka",
               "http": "http", "sftp": "sftp", "cloudsql": "cloudsql"}


@lru_cache(maxsize=1)
def _resources_root() -> str | None:
    spec = importlib.util.find_spec("diagrams")
    if not spec or not spec.submodule_search_locations:
        return None
    pkg = list(spec.submodule_search_locations)[0]
    for cand in (os.path.join(pkg, "..", "resources"), os.path.join(pkg, "resources")):
        if os.path.isdir(cand):
            return os.path.normpath(cand)
    return None


@lru_cache(maxsize=128)
def icon_uri(kind: str | None) -> str | None:
    """Data-URI PNG del ícono del tipo de recurso, o None si no hay ícono (el diagrama usa un punto de color)."""
    root = _resources_root()
    rel = ICON_FILES.get(kind or "")
    if not root or not rel:
        return None
    path = os.path.join(root, rel)
    if not os.path.isfile(path):
        return None
    with open(path, "rb") as fh:
        return "data:image/png;base64," + base64.b64encode(fh.read()).decode()


def icons_available() -> bool:
    return _resources_root() is not None
