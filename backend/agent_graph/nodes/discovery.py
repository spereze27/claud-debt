"""Nodo 1 — Discovery real de GCP.

Extrae inventario vía SDKs oficiales y lo normaliza al MISMO esquema que ya
consumen los agentes (iam_roles, storage_buckets, compute_instances,
block_volumes, static_ips, snapshots), así Security/Governance/Cost no cambian.

Cada colector es independiente: si uno falla (p.ej. PermissionDenied), se
registra el error con el permiso IAM necesario, esa sección queda vacía y el
resto del grafo sigue su curso.

Modo demo: DISCOVERY_MODE=mock usa el generador simulado (útil para tests).
"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

from backend.agent_graph.nodes.discovery_mock import MockDiscoveryProvider
from backend.agent_graph.state import AuditState

logger = logging.getLogger(__name__)

SCOPES = ["https://www.googleapis.com/auth/cloud-platform"]
PUBLIC_MEMBERS = {"allUsers", "allAuthenticatedUsers"}

# Precios de lista aproximados (USD/mes, us-central1). Sólo para priorizar ahorro.
DISK_PRICE_GB = {"pd-standard": 0.04, "pd-balanced": 0.10, "pd-ssd": 0.17, "pd-extreme": 0.125,
                 "hyperdisk-balanced": 0.08}
SNAPSHOT_PRICE_GB = 0.05
UNUSED_IP_MONTHLY = 7.30

# Permiso mínimo que necesita cada colector: se muestra en el informe si falla.
REQUIRED_PERMISSIONS = {
    "iam": "resourcemanager.projects.getIamPolicy (rol roles/iam.securityReviewer)",
    "iam_keys": "iam.serviceAccounts.list / iam.serviceAccountKeys.list (rol roles/iam.securityReviewer)",
    "storage": "storage.buckets.list y storage.buckets.getIamPolicy (rol roles/storage.admin o roles/cloudasset.viewer)",
    "compute_instances": "compute.instances.list (rol roles/compute.viewer)",
    "compute_disks": "compute.disks.list (rol roles/compute.viewer)",
    "compute_addresses": "compute.addresses.list (rol roles/compute.viewer)",
    "compute_snapshots": "compute.snapshots.list (rol roles/compute.viewer)",
    "compute_firewalls": "compute.firewalls.list (rol roles/compute.viewer)",
    "monitoring": "monitoring.timeSeries.list (rol roles/monitoring.viewer)",
    "asset_inventory": "cloudasset.assets.listResource (rol roles/cloudasset.viewer) y API cloudasset.googleapis.com habilitada",
    "asset_iam": "cloudasset.assets.listIamPolicy (rol roles/cloudasset.viewer)",
    "billing": "resourcemanager.projects.get + billing.resourceAssociations.list (rol roles/billing.viewer en la cuenta de facturación) y API cloudbilling.googleapis.com",
    "bigquery_metadata": "bigquery.datasets.get + bigquery.tables.list + bigquery.jobs.create (roles/bigquery.metadataViewer y roles/bigquery.jobUser)",
    "bigquery_jobs": "bigquery.jobs.listAll (rol roles/bigquery.resourceViewer) para leer INFORMATION_SCHEMA.JOBS_BY_PROJECT",
    "bigquery_transfers": "bigquery.transfers.get (rol roles/bigquery.user) y API bigquerydatatransfer.googleapis.com",
    "dataflow": "dataflow.jobs.list/get (rol roles/dataflow.viewer)",
    "dataproc": "dataproc.clusters.list + dataproc.jobs.list (rol roles/dataproc.viewer)",
    "composer": "composer.environments.get (rol roles/composer.user)",
    "composer_dags": "storage.objects.get/list en el bucket de DAGs (rol roles/composer.environmentAndStorageObjectViewer)",
    "data_lake": "storage.objects.list en los buckets del lake (rol roles/storage.objectViewer)",
    "capacity": "monitoring.timeSeries.list (rol roles/monitoring.viewer)",
    "services_metrics": "monitoring.timeSeries.list (rol roles/monitoring.viewer)",
    "datastream": "cloudasset.assets.listResource (rol roles/cloudasset.viewer)",
    "data_platform": "varios (ver avisos anteriores)",
    "projects": "resourcemanager.projects.list/folders.list (rol roles/browser en la carpeta u organización)",
    "billing_export": "bigquery.tables.getData + bigquery.jobs.create (roles/bigquery.dataViewer en el dataset del export y roles/bigquery.jobUser)",
}


class DiscoveryError(Exception):
    """Error que impide cualquier descubrimiento (credenciales inválidas, alcance no soportado...)."""


# --------------------------------------------------------------------- utilidades
def _last(url: str | None) -> str | None:
    """'.../zones/us-central1-a/machineTypes/e2-medium' → 'e2-medium'."""
    return url.rsplit("/", 1)[-1] if url else None


def _parse_ts(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


GCP_MODULES = ("google.cloud.asset_v1", "google.cloud.billing_v1", "google.api_core", "google.oauth2", "google.cloud.storage", "google.cloud.resourcemanager_v3",
               "google.cloud.compute_v1", "google.cloud.iam_admin_v1", "google.cloud.monitoring_v3")
PIP_HINT = "python -m pip install -r requirements.txt"


def missing_gcp_modules() -> list[str]:
    """Módulos del SDK de GCP que no están instalados en ESTE intérprete de Python."""
    import importlib.util

    missing = []
    for mod in GCP_MODULES:
        try:
            if importlib.util.find_spec(mod) is None:
                missing.append(mod)
        except ModuleNotFoundError:
            missing.append(mod)
    return missing


def _describe_error(section: str, exc: Exception) -> str:
    if isinstance(exc, ModuleNotFoundError):
        return f"FALTAN LIBRERÍAS en '{section}': {exc}. Instálalas en el entorno del backend: {PIP_HINT}"
    try:
        from google.api_core import exceptions as gexc
    except ModuleNotFoundError:
        return f"ERROR en '{section}': {type(exc).__name__}: {str(exc)[:300]}"

    if isinstance(exc, (gexc.PermissionDenied, gexc.Forbidden)):
        return (f"PERMISO DENEGADO en '{section}': la Service Account no tiene "
                f"{REQUIRED_PERMISSIONS.get(section, 'los permisos necesarios')}. Esta sección no fue auditada.")
    if isinstance(exc, gexc.NotFound):
        return f"NO ENCONTRADO en '{section}': verifica que el Project ID exista. ({exc.message})"
    if isinstance(exc, gexc.Unauthenticated):
        return f"CREDENCIALES INVÁLIDAS en '{section}': la clave de la Service Account fue rechazada."
    if isinstance(exc, gexc.GoogleAPICallError) and ("has not been used" in str(exc) or "is disabled" in str(exc)):
        return f"API DESHABILITADA en '{section}': habilita la API correspondiente en el proyecto. ({exc})"
    return f"ERROR en '{section}': {type(exc).__name__}: {str(exc)[:300]}"


@dataclass
class CollectionResult:
    data: dict[str, Any] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)
    sections_ok: list[str] = field(default_factory=list)


# --------------------------------------------------------------------- colector GCP
def load_credentials(credentials_path: str | None) -> Any:
    from google.oauth2 import service_account

    if not credentials_path or not os.path.isfile(credentials_path):
        raise DiscoveryError(f"No se encontró el archivo de credenciales: {credentials_path!r}")
    try:
        return service_account.Credentials.from_service_account_file(credentials_path, scopes=SCOPES)
    except (ValueError, KeyError) as exc:
        raise DiscoveryError(f"El JSON no es una clave válida de Service Account: {exc}") from exc


class GCPDiscoveryProvider:
    """Descubrimiento real de un proyecto GCP (lectura únicamente)."""

    def __init__(self, credentials_path: str | None, project_id: str, billing_export_table: str | None = None,
                 data_options: dict[str, Any] | None = None, credentials: Any = None) -> None:
        self.credentials = credentials or load_credentials(credentials_path)
        self.data_options = data_options or {}
        self.project_id = project_id
        self.billing_export_table = billing_export_table
        self.now = datetime.now(timezone.utc)
        self._instance_tags: dict[str, dict[str, Any]] = {}

    # ---- orquestación -------------------------------------------------------
    def discover(self) -> CollectionResult:
        result = CollectionResult()
        steps: list[tuple[str, str, Callable[[], Any]]] = [
            ("iam", "iam_roles", self._collect_iam),
            ("storage", "storage_buckets", self._collect_buckets),
            ("compute_firewalls", "_firewalls", self._collect_firewalls),
            ("compute_instances", "compute_instances", self._collect_instances),
            ("compute_disks", "block_volumes", self._collect_disks),
            ("compute_addresses", "static_ips", self._collect_addresses),
            ("compute_snapshots", "snapshots", self._collect_snapshots),
        ]
        for section, key, fn in steps:
            try:
                result.data[key] = fn() if key != "compute_instances" else fn(result.data.get("_firewalls", []))
                result.sections_ok.append(section)
            except DiscoveryError:
                raise
            except Exception as exc:  # noqa: BLE001 — aislar cada colector
                msg = _describe_error(section, exc)
                logger.error("Discovery GCP: %s", msg)
                result.errors.append(msg)
                result.data.setdefault(key, [])

        # Enriquecimientos opcionales (no bloquean): antigüedad de keys y CPU 30d.
        for section, fn in (("iam_keys", lambda: self._enrich_sa_keys(result.data["iam_roles"])),
                            ("monitoring", lambda: self._enrich_cpu(result.data["compute_instances"]))):
            try:
                fn()
                result.sections_ok.append(section)
            except Exception as exc:  # noqa: BLE001
                msg = _describe_error(section, exc)
                logger.warning("Discovery GCP (enriquecimiento): %s", msg)
                result.errors.append(msg)

        result.data.pop("_firewalls", None)

        # Arquitectura completa (Cloud Run, Functions, Pub/Sub, SQL, Secrets, facturación...) + grafo.
        from backend.agent_graph.nodes.gcp_architecture import GCPArchitectureCollector

        try:
            arch, errs, ok = GCPArchitectureCollector(self.credentials, self.project_id,
                                                      self.billing_export_table).collect(
                result.data.get("iam_roles", []), _describe_error)
            result.data.update(arch)
            result.errors += errs
            result.sections_ok += ok
        except Exception as exc:  # noqa: BLE001
            logger.exception("Arquitectura GCP")
            result.errors.append(_describe_error("asset_inventory", exc))
        assets = result.data.pop("_assets", [])

        # Plataforma de datos: flujos, linaje, gobierno de datos, data lake, capacidad y costos por flujo.
        if self.data_options.get("enabled", True):
            self._collect_data_platform(result, assets)
        return result

    def _collect_data_platform(self, result: CollectionResult, assets: list[dict[str, Any]]) -> None:
        from backend.agent_graph.nodes.gcp_data import GCPDataCollector
        from backend.core.data_platform import build_data_platform
        from backend.core.rightsizing import size_all

        opts = self.data_options
        buckets = [b["resource_id"] for b in result.data.get("storage_buckets", [])]
        buckets += [n["label"] for n in (result.data.get("architecture") or {}).get("nodes", [])
                    if n["type"] == "Cloud Storage"]
        try:
            collector = GCPDataCollector(self.credentials, self.project_id, assets, result.data,
                                         history_days=int(opts.get("history_days", 30)),
                                         sample_lake=bool(opts.get("sample_lake", True)),
                                         read_dags=bool(opts.get("read_dags", True)), known_buckets=buckets)
            raw, errs, ok = collector.collect(_describe_error)
            result.errors += errs
            result.sections_ok += ok
            sized = size_all(raw.pop("clusters", []))
            catalog = [c for c in opts.get("catalog") or []
                       if not c.get("project") or c.get("project") == self.project_id]
            bucket_labels = {b["resource_id"]: b.get("tags") or {} for b in result.data.get("storage_buckets", [])}
            dp = build_data_platform({**raw, "clusters_sized": sized, "catalog": catalog, "bucket_labels": bucket_labels},
                                     int(opts.get("history_days", 30)))
            dp["clusters"] = sized
            result.data["data_platform"] = dp
        except Exception as exc:  # noqa: BLE001
            logger.exception("Plataforma de datos")
            result.errors.append(_describe_error("data_platform", exc))

    # ---- IAM ----------------------------------------------------------------
    def _collect_iam(self) -> list[dict[str, Any]]:
        from google.cloud import resourcemanager_v3

        client = resourcemanager_v3.ProjectsClient(credentials=self.credentials)
        policy = client.get_iam_policy(request={"resource": f"projects/{self.project_id}",
                                                "options": {"requested_policy_version": 3}})
        members: dict[str, dict[str, Any]] = {}
        for binding in policy.bindings:
            for member in binding.members:
                entry = members.setdefault(member, {
                    "resource_id": member,
                    "name": member.split(":", 1)[-1],
                    "type": member.split(":", 1)[0],  # user | serviceAccount | group | domain | deleted
                    "attached_policies": [],
                    "conditional_roles": [],
                    "access_key_age_days": None,
                    "user_managed_keys": None,
                    "last_used_days_ago": None,
                    "mfa_enabled": None,  # no expuesto por la API de GCP (se gestiona en Workspace/Cloud Identity)
                })
                target = "conditional_roles" if binding.condition and binding.condition.expression else "attached_policies"
                entry[target].append(binding.role)
        return list(members.values())

    def _enrich_sa_keys(self, roles: list[dict[str, Any]]) -> None:
        """Edad de la clave user-managed más antigua de cada Service Account."""
        from google.cloud import iam_admin_v1

        client = iam_admin_v1.IAMClient(credentials=self.credentials)
        for sa in client.list_service_accounts(request={"name": f"projects/{self.project_id}"}):
            member = f"serviceAccount:{sa.email}"
            keys = client.list_service_account_keys(request={
                "name": sa.name, "key_types": [iam_admin_v1.ListServiceAccountKeysRequest.KeyType.USER_MANAGED]}).keys
            ages = [(self.now - k.valid_after_time).days for k in keys if k.valid_after_time]
            entry = next((r for r in roles if r["resource_id"] == member), None)
            if entry is None:  # SA sin roles a nivel proyecto: igual interesa por sus claves
                entry = {"resource_id": member, "name": sa.email, "type": "serviceAccount",
                         "attached_policies": [], "conditional_roles": [], "last_used_days_ago": None,
                         "mfa_enabled": None}
                roles.append(entry)
            entry["user_managed_keys"] = len(keys)
            entry["access_key_age_days"] = max(ages) if ages else None
            entry["disabled"] = sa.disabled

    # ---- Cloud Storage ------------------------------------------------------
    def _collect_buckets(self) -> list[dict[str, Any]]:
        from google.api_core import exceptions as gexc
        from google.cloud import storage

        client = storage.Client(project=self.project_id, credentials=self.credentials)
        buckets = []
        for b in client.list_buckets():
            iam_cfg = b.iam_configuration
            pap = getattr(iam_cfg, "public_access_prevention", None)
            public_members: list[str] = []
            try:
                policy = b.get_iam_policy(requested_policy_version=3)
                public_members = sorted({f"{m} → {bind['role']}" for bind in policy.bindings
                                         for m in bind.get("members", []) if m in PUBLIC_MEMBERS})
            except (gexc.Forbidden, gexc.PermissionDenied):
                public_members = ["<desconocido: sin permiso storage.buckets.getIamPolicy>"]
            is_public = pap != "enforced" and any(not m.startswith("<") for m in public_members)
            labels = dict(b.labels or {})
            buckets.append({
                "resource_id": b.name,
                "region": b.location,
                "storage_class": b.storage_class,
                "public_access": is_public,
                "public_bindings": public_members,
                "public_access_prevention": pap,
                "uniform_bucket_level_access": bool(getattr(iam_cfg, "uniform_bucket_level_access_enabled", False)),
                # GCS siempre cifra en reposo; distinguimos clave de Google vs CMEK.
                "encryption": "CMK" if b.default_kms_key_name else "PROVIDER_MANAGED",
                "kms_key": b.default_kms_key_name,
                "versioning": bool(b.versioning_enabled),
                "retention_policy": bool(b.retention_period),
                "size_gb": None,  # requiere Cloud Monitoring (storage/total_bytes)
                "data_classification": labels.get("data-classification") or labels.get("data_classification"),
                "tags": labels,
                "created_at": b.time_created.isoformat() if b.time_created else None,
                "monthly_cost_usd": None,
            })
        return buckets

    # ---- Compute Engine -----------------------------------------------------
    def _collect_firewalls(self) -> list[dict[str, Any]]:
        from google.cloud import compute_v1

        rules = []
        for fw in compute_v1.FirewallsClient(credentials=self.credentials).list(project=self.project_id):
            if fw.direction != "INGRESS" or fw.disabled:
                continue
            ports = [p for a in fw.allowed for p in (list(a.ports) or [f"all/{a.I_p_protocol}"])]
            rules.append({"name": fw.name, "network": _last(fw.network), "source_ranges": list(fw.source_ranges),
                          "target_tags": set(fw.target_tags), "target_sas": set(fw.target_service_accounts),
                          "ports": ports})
        return rules

    def _collect_instances(self, firewalls: list[dict[str, Any]]) -> list[dict[str, Any]]:
        from google.cloud import compute_v1

        client = compute_v1.InstancesClient(credentials=self.credentials)
        out = []
        for zone, scoped in client.aggregated_list(request={"project": self.project_id}):
            for inst in scoped.instances:
                nic_networks = {_last(n.network) for n in inst.network_interfaces}
                public_ip = next((ac.nat_i_p for n in inst.network_interfaces for ac in n.access_configs
                                  if ac.nat_i_p), None)
                tags = set(inst.tags.items) if inst.tags else set()
                sas = {s.email for s in inst.service_accounts}
                ingress = [
                    {"port": port, "source": src, "rule": fw["name"]}
                    for fw in firewalls
                    if fw["network"] in nic_networks
                    and (not fw["target_tags"] and not fw["target_sas"]
                         or fw["target_tags"] & tags or fw["target_sas"] & sas)
                    for src in fw["source_ranges"] for port in fw["ports"]
                ]
                out.append({
                    "resource_id": inst.name,
                    "zone": _last(zone),
                    "machine_type": _last(inst.machine_type),
                    "state": inst.status,
                    "public_ip": public_ip,
                    "firewall_ingress": ingress,
                    "service_accounts": [{"email": s.email, "scopes": list(s.scopes)} for s in inst.service_accounts],
                    "deletion_protection": inst.deletion_protection,
                    "avg_cpu_30d_pct": None,  # se rellena con Cloud Monitoring si hay permiso
                    "tags": dict(inst.labels or {}),
                    "network_tags": sorted(tags),
                    "created_at": inst.creation_timestamp,
                    "monthly_cost_usd": None,  # el costo real requiere export de Billing a BigQuery
                    "_id": str(inst.id),
                })
        return out

    def _enrich_cpu(self, instances: list[dict[str, Any]]) -> None:
        """CPU media 30 días desde Cloud Monitoring (para detectar instancias ociosas)."""
        from google.cloud import monitoring_v3

        if not instances:
            return
        client = monitoring_v3.MetricServiceClient(credentials=self.credentials)
        end = self.now
        request = {
            "name": f"projects/{self.project_id}",
            "filter": 'metric.type = "compute.googleapis.com/instance/cpu/utilization"',
            "interval": monitoring_v3.TimeInterval(start_time=end - timedelta(days=30), end_time=end),
            "aggregation": monitoring_v3.Aggregation(
                alignment_period={"seconds": 30 * 24 * 3600},
                per_series_aligner=monitoring_v3.Aggregation.Aligner.ALIGN_MEAN),
            "view": monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
        }
        by_id = {i["_id"]: i for i in instances}
        for series in client.list_time_series(request=request):
            inst = by_id.get(series.resource.labels.get("instance_id", ""))
            if inst and series.points:
                inst["avg_cpu_30d_pct"] = round(series.points[0].value.double_value * 100, 1)
        for i in instances:
            i.pop("_id", None)

    def _collect_disks(self) -> list[dict[str, Any]]:
        from google.cloud import compute_v1

        out = []
        for zone, scoped in compute_v1.DisksClient(credentials=self.credentials).aggregated_list(
                request={"project": self.project_id}):
            for d in scoped.disks:
                users = [_last(u) for u in d.users]
                detached = _parse_ts(d.last_detach_timestamp) or _parse_ts(d.creation_timestamp)
                disk_type = _last(d.type_) or "pd-standard"
                kms = d.disk_encryption_key.kms_key_name if d.disk_encryption_key else None
                out.append({
                    "resource_id": d.name,
                    "zone": _last(zone),
                    "disk_type": disk_type,
                    "size_gb": int(d.size_gb),
                    "attached_to": users[0] if users else None,
                    "attached_users": users,
                    "encrypted": True,  # PD siempre cifrado en reposo
                    "encryption": "CMK" if kms else "PROVIDER_MANAGED",
                    "tags": dict(d.labels or {}),
                    "last_attached_days_ago": 0 if users else ((self.now - detached).days if detached else None),
                    "monthly_cost_usd": round(int(d.size_gb) * DISK_PRICE_GB.get(disk_type, 0.04), 2),
                })
        # Rellenar attached_to en instancias ya no es necesario; los agentes cruzan por resource_id.
        return out

    def _collect_addresses(self) -> list[dict[str, Any]]:
        from google.cloud import compute_v1

        out = []
        for region, scoped in compute_v1.AddressesClient(credentials=self.credentials).aggregated_list(
                request={"project": self.project_id}):
            for a in scoped.addresses:
                users = [_last(u) for u in a.users]
                out.append({
                    "resource_id": a.name,
                    "address": a.address,
                    "region": _last(region),
                    "address_type": a.address_type,  # EXTERNAL | INTERNAL
                    "status": a.status,              # RESERVED (huérfana) | IN_USE
                    "associated_with": users[0] if users else None,
                    "monthly_cost_usd": UNUSED_IP_MONTHLY if a.status == "RESERVED" and a.address_type == "EXTERNAL" else 0.0,
                })
        return out

    def _collect_snapshots(self) -> list[dict[str, Any]]:
        from google.cloud import compute_v1

        out = []
        for s in compute_v1.SnapshotsClient(credentials=self.credentials).list(project=self.project_id):
            created = _parse_ts(s.creation_timestamp)
            size_gb = round(int(s.storage_bytes) / 1024 ** 3, 2) if s.storage_bytes else int(s.disk_size_gb)
            out.append({
                "resource_id": s.name,
                "source_volume": _last(s.source_disk),
                "created_at": created.date().isoformat() if created else None,
                "age_days": (self.now - created).days if created else None,
                "size_gb": size_gb,
                "monthly_cost_usd": round(size_gb * SNAPSHOT_PRICE_GB, 2),
            })
        return out


def _mark_orphan_snapshots(data: dict[str, Any]) -> None:
    """Marca snapshots cuyo disco origen ya no existe (dato objetivo para el Cost_Analyzer)."""
    existing = {d["resource_id"] for d in data.get("block_volumes", [])}
    for s in data.get("snapshots", []):
        s["source_volume_exists"] = s["source_volume"] in existing if data.get("block_volumes") is not None else None


# --------------------------------------------------------------------- nodo
def list_projects(credentials: Any, scope: str, target_id: str, max_projects: int) -> list[dict[str, str]]:
    """Proyectos ACTIVOS bajo una carpeta u organización (recorre subcarpetas). Devuelve [{project_id, folder_path}]."""
    from google.cloud import resourcemanager_v3 as rm

    projects_client = rm.ProjectsClient(credentials=credentials)
    folders_client = rm.FoldersClient(credentials=credentials)
    root = f"{'folders' if scope == 'folder' else 'organizations'}/{target_id}"
    out: list[dict[str, str]] = []
    queue: list[tuple[str, str]] = [(root, target_id)]
    while queue and len(out) < max_projects:
        parent, path = queue.pop(0)
        for p in projects_client.list_projects(parent=parent, timeout=30):
            if p.state.name == "ACTIVE" and not p.project_id.startswith("sys-"):
                out.append({"project_id": p.project_id, "folder_path": path, "display_name": p.display_name})
                if len(out) >= max_projects:
                    break
        try:
            for f in folders_client.list_folders(parent=parent, timeout=30):
                if f.state.name == "ACTIVE":
                    queue.append((f.name, f"{path} / {f.display_name}"))
        except Exception as exc:  # noqa: BLE001
            logger.info("Subcarpetas de %s: %s", parent, exc)
    return out


PREFIXED_LISTS = ("iam_roles", "compute_instances", "block_volumes", "static_ips", "snapshots", "workloads",
                  "databases", "other_resources")


def merge_projects(results: list[dict[str, Any]]) -> dict[str, Any]:
    """Une el inventario de varios proyectos. Los ids que podrían colisionar se prefijan con el proyecto."""
    from backend.core.data_platform import summarize

    merged: dict[str, Any] = {"projects": [], "architecture": {"nodes": [], "edges": []},
                              "data_platform": {"tables": [], "datasets": [], "lake": [], "flows": [], "issues": [],
                                                "dependencies": [], "cascades": [],
                                                "clusters": [], "lineage": {"nodes": [], "edges": []},
                                                "cost_breakdown_by_project": []},
                              "enabled_apis": set(), "asset_counts": {}, "billing_by_project": []}
    multi = len(results) > 1
    seen_nodes: set[str] = set()
    for r in results:
        pid, data = r["project_id"], r["data"]
        pre = (lambda x: f"{pid}/{x}") if multi else (lambda x: x)
        merged["projects"].append({k: r.get(k) for k in ("project_id", "folder_path", "display_name", "status")}
                                  | {"errors": len(r.get("errors") or [])})
        for key in PREFIXED_LISTS:
            for item in data.get(key) or []:
                merged.setdefault(key, []).append({**item, "resource_id": pre(item["resource_id"]), "project_id": pid})
        for key in ("storage_buckets", "service_identities"):
            for item in data.get(key) or []:
                extra = {"used_by": [pre(u) for u in item.get("used_by", [])]} if key == "service_identities" else {}
                merged.setdefault(key, []).append({**item, **extra, "project_id": pid})
        arch = data.get("architecture") or {}
        for n in arch.get("nodes") or []:
            nid = n["id"] if n["id"] == "internet" else pre(n["id"])
            if nid not in seen_nodes:
                seen_nodes.add(nid)
                merged["architecture"]["nodes"].append({**n, "id": nid, "project_id": pid})
        for e in arch.get("edges") or []:
            merged["architecture"]["edges"].append({**e, "source": e["source"] if e["source"] == "internet" else pre(e["source"]),
                                                    "target": e["target"] if e["target"] == "internet" else pre(e["target"])})
        merged["enabled_apis"] |= set(data.get("enabled_apis") or [])
        for t, n in (data.get("asset_counts") or {}).items():
            merged["asset_counts"][t] = merged["asset_counts"].get(t, 0) + n
        if data.get("billing"):
            merged["billing_by_project"].append({**data["billing"], "project_id": pid})
        dp = data.get("data_platform") or {}
        for key in ("tables", "datasets", "lake", "flows", "issues", "clusters", "dependencies", "cascades"):
            merged["data_platform"][key] += [{**x, "folder_path": r.get("folder_path")} if key == "flows" else x
                                             for x in dp.get(key) or []]
        merged["data_platform"]["lineage"]["nodes"] += (dp.get("lineage") or {}).get("nodes") or []
        merged["data_platform"]["lineage"]["edges"] += (dp.get("lineage") or {}).get("edges") or []
        if dp.get("cost_breakdown"):
            merged["data_platform"]["cost_breakdown_by_project"].append(
                {**dp["cost_breakdown"], "folder_path": r.get("folder_path")})
        merged["data_platform"]["history_days"] = dp.get("history_days")
    merged["enabled_apis"] = sorted(merged["enabled_apis"])
    bills = merged["billing_by_project"]
    if bills:
        merged["billing"] = {
            "resource_id": "billing:" + ("multi" if multi else bills[0]["project_id"]),
            "billing_enabled": any(b.get("billing_enabled") for b in bills),
            "has_budget": (None if any(b.get("has_budget") is None for b in bills)
                           else all(b.get("has_budget") for b in bills)),
            "billing_account": ", ".join(sorted({b.get("billing_account") or "" for b in bills} - {""})) or None,
            "budgets": [x for b in bills for x in (b.get("budgets") or [])],
            "total_cost_30d_usd": (round(sum(b.get("total_cost_30d_usd") or 0 for b in bills), 2)
                                   if any(b.get("total_cost_30d_usd") is not None for b in bills) else None),
            "cost_by_service_30d": [x for b in bills for x in (b.get("cost_by_service_30d") or [])] or None,
            "projects_without_budget": [b["project_id"] for b in bills if b.get("has_budget") is False],
        }
    dpm = merged["data_platform"]
    seen, uniq = set(), []
    for n in dpm["lineage"]["nodes"]:
        if n["id"] not in seen:
            seen.add(n["id"])
            uniq.append(n)
    dpm["lineage"]["nodes"] = uniq
    dpm["summary"] = summarize(dpm["tables"], dpm["flows"], dpm["lake"], dpm["issues"])
    cbs = dpm["cost_breakdown_by_project"]
    dpm["cost_breakdown"] = {"total_usd": round(sum(c["total_usd"] for c in cbs), 2),
                             "flows_total_usd": round(sum(c["flows_total_usd"] for c in cbs), 2),
                             "shared_total_usd": round(sum(c["shared_total_usd"] for c in cbs), 2)}
    for r in results:  # el resto de claves de un solo proyecto (mock u otras) se conservan
        for k, v in r["data"].items():
            merged.setdefault(k, v)
    return merged


def _discover_one(credentials: Any, project_id: str, state: AuditState, data_options: dict[str, Any]) -> dict[str, Any]:
    result = GCPDiscoveryProvider(None, project_id, state.get("billing_export_table"), data_options,
                                  credentials=credentials).discover()
    _mark_orphan_snapshots(result.data)
    status = "complete" if not result.errors else ("partial" if result.sections_ok else "failed")
    return {"project_id": project_id, "data": result.data, "errors": result.errors,
            "sections_ok": result.sections_ok, "status": status}


def discovery_node(state: AuditState) -> dict[str, Any]:
    provider, scope, target = state["provider"], state["scope"], state["target_id"]
    logger.info("Discovery: %s/%s/%s", provider, scope, target)

    if os.getenv("DISCOVERY_MODE", "real").lower() == "mock":
        return {"raw_infra_data": MockDiscoveryProvider(provider).discover(scope, target),
                "discovery_status": "mock"}

    base_meta = {"provider": provider, "scope": scope, "target_id": target,
                 "collected_at": datetime.now(timezone.utc).isoformat(), "collector": "gcp-sdk-v1",
                 "required_tags": ["owner", "environment", "cost-center", "application"]}

    def failed(msg: str) -> dict[str, Any]:
        logger.error("Discovery abortado: %s", msg)
        return {"raw_infra_data": {"metadata": {**base_meta, "collection_errors": [msg]}},
                "discovery_status": "failed", "errors": [f"discovery: {msg}"]}

    if provider != "GCP":
        return failed(f"El descubrimiento real sólo está implementado para GCP (recibido: {provider}).")
    if missing := missing_gcp_modules():
        return failed(f"Faltan librerías de GCP en el entorno del backend ({', '.join(missing)}). "
                      f"Ejecuta en ese mismo entorno: {PIP_HINT}")
    if not state.get("credentials_path"):
        return failed("No se proporcionó el archivo JSON de la Service Account.")

    opts = dict(state.get("data_options") or {})
    try:
        credentials = load_credentials(state["credentials_path"])
        if scope == "project":
            targets = [{"project_id": target, "folder_path": None, "display_name": None}]
        else:
            targets = list_projects(credentials, scope, target, int(opts.get("max_projects", 10)))
            if not targets:
                return failed(f"No se encontraron proyectos activos bajo {scope} {target} "
                              "(o la Service Account no tiene roles/browser ahí).")
    except DiscoveryError as exc:
        return failed(str(exc))
    except Exception as exc:  # noqa: BLE001
        return failed(_describe_error("projects", exc))

    from concurrent.futures import ThreadPoolExecutor

    results = []
    with ThreadPoolExecutor(max_workers=min(4, len(targets))) as pool:
        futures = {pool.submit(_discover_one, credentials, t["project_id"], state, opts): t for t in targets}
        for fut, t in futures.items():
            try:
                results.append({**fut.result(), "folder_path": t["folder_path"], "display_name": t["display_name"]})
            except Exception as exc:  # noqa: BLE001 — un proyecto roto no tumba los demás
                logger.exception("Discovery de %s", t["project_id"])
                results.append({"project_id": t["project_id"], "data": {}, "errors": [f"{type(exc).__name__}: {exc}"],
                                "sections_ok": [], "status": "failed", "folder_path": t["folder_path"]})

    statuses = {r["status"] for r in results}
    status = "complete" if statuses == {"complete"} else "failed" if statuses == {"failed"} else "partial"
    multi = len(results) > 1
    errors = [(f"[{r['project_id']}] " if multi else "") + e for r in results for e in r["errors"]]
    merged = merge_projects(results)
    raw = {"metadata": {**base_meta, "projects_analyzed": len(results),
                        "sections_ok": sorted({s for r in results for s in r["sections_ok"]}),
                        "collection_errors": errors,
                        "cost_note": "Costos estimados con precios de lista de GCP (us-central1); "
                                     "configura el export de facturación para ver el costo real."},
           **merged}
    logger.info("Discovery GCP %s: %d proyecto(s)", status, len(results))
    return {"raw_infra_data": raw, "discovery_status": status, "errors": [f"discovery: {e}" for e in errors]}
