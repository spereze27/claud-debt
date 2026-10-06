"""Definición del estado compartido del grafo LangGraph."""
from __future__ import annotations

import operator
from typing import Annotated, Any, Literal, TypedDict

Provider = Literal["AWS", "GCP", "Azure"]
DiscoveryStatus = Literal["complete", "partial", "failed", "mock"]
Scope = Literal["organization", "folder", "project"]
Severity = Literal["CRITICAL", "HIGH", "MEDIUM", "LOW"]
Category = Literal["security", "governance", "cost", "data_governance", "pipelines", "capacity"]


class Finding(TypedDict):
    """Hallazgo individual producido por un agente especialista."""

    id: str
    category: Category
    rule: str                # identificador de la regla, p.ej. "IAM_ADMIN_ROLE"
    resource_id: str
    resource_type: str
    severity: Severity
    title: str
    description: str
    recommendation: str
    estimated_monthly_waste_usd: float


class AuditState(TypedDict, total=False):
    """Estado del grafo.

    `findings` y `errors` usan el reducer `operator.add` para que los tres
    analizadores paralelos puedan escribir sin pisarse (fan-in seguro).

    La API Key del LLM NO vive en el estado: se inyecta vía
    `config["configurable"]` para que no quede persistida en checkpoints.
    """

    provider: Provider
    scope: Scope
    target_id: str
    has_credentials: bool
    # Ruta a un archivo TEMPORAL con el JSON de la Service Account. Lo crea la API
    # (permisos 0600) y lo elimina al terminar; el contenido nunca entra al estado.
    credentials_path: str | None
    billing_export_table: str | None   # opcional: proyecto.dataset.tabla del export de facturación
    # Opciones del análisis de datos: history_days, sample_lake, read_dags, max_projects, catalog (flujos externos)
    data_options: dict[str, Any]
    discovery_status: DiscoveryStatus
    raw_infra_data: dict[str, Any]
    findings: Annotated[list[Finding], operator.add]
    errors: Annotated[list[str], operator.add]
    kpis: dict[str, Any]
    final_report: str
    data_report: str          # informe detallado de datos, flujos, capacidad y costos por flujo
    excel_report: dict[str, Any] | None   # {report_id, path, filename, sheets} del .xlsx exportado
