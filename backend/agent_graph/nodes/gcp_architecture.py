"""Descubrimiento de ARQUITECTURA completa de un proyecto GCP.

Usa Cloud Asset Inventory (una sola API) para listar TODOS los recursos y sus
políticas IAM a nivel recurso, y construye un grafo de relaciones:

    recurso ──(mecanismo, identidad, permisos)──► recurso

Mecanismos detectados:
  runs_as        workload → cuenta de servicio con la que se ejecuta
  invokes        Cloud Scheduler → Cloud Run/Functions (HTTP con token OIDC)
  push           Pub/Sub subscription → endpoint HTTP (con token OIDC)
  subscribes     subscription → topic
  triggers       Eventarc / topic → workload
  publishes      Scheduler → topic
  connects_sql   workload → Cloud SQL (conector Cloud SQL)
  reads_secret   workload → Secret Manager
  uses_vpc       workload → Serverless VPC connector
  references     workload → recurso cuyo nombre aparece en sus variables de entorno (INFERIDO)
  has_role       identidad → recurso (binding IAM a nivel recurso)
  public_access  internet → recurso (allUsers / allAuthenticatedUsers)

El módulo separa la RECOLECCIÓN (llamadas a la API) del PARSEO (funciones
puras sobre dicts) para poder testearlo sin red.
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Iterable

logger = logging.getLogger(__name__)

PUBLIC_MEMBERS = {"allUsers", "allAuthenticatedUsers"}
BROAD_ROLES = {"roles/owner", "roles/editor"}
PRIV_ESC_ROLES = {"roles/iam.serviceAccountUser", "roles/iam.serviceAccountTokenCreator",
                  "roles/iam.serviceAccountKeyAdmin", "roles/resourcemanager.projectIamAdmin",
                  "roles/iam.securityAdmin"}
SECRET_NAME_RE = re.compile(r"(PASS(WORD)?|PWD|SECRET|TOKEN|API[_-]?KEY|PRIVATE[_-]?KEY|CREDENTIAL|AUTH)", re.I)
BILLING_TABLE_RE = re.compile(r"^[a-z][a-z0-9-]{4,61}[a-z0-9]\.[A-Za-z0-9_]{1,1024}\.[A-Za-z0-9_$\-]{1,1024}$")

# Tipos que se consideran "workloads" (ejecutan código con una identidad).
WORKLOAD_TYPES = {"Cloud Run", "Cloud Functions", "Compute Engine", "GKE", "Workflows", "App Engine"}

# Tipos de asset que ignoramos para no llenar el grafo de ruido de red/infra base.
NOISE_ASSET_PREFIXES = (
    "compute.googleapis.com/Route", "compute.googleapis.com/Subnetwork", "compute.googleapis.com/Firewall",
    "compute.googleapis.com/Network", "compute.googleapis.com/Project", "logging.googleapis.com/",
    "monitoring.googleapis.com/", "cloudresourcemanager.googleapis.com/", "run.googleapis.com/Revision",
    "run.googleapis.com/DomainMapping", "artifactregistry.googleapis.com/DockerImage", "iam.googleapis.com/Role",
    "iam.googleapis.com/ServiceAccountKey", "compute.googleapis.com/Disk", "compute.googleapis.com/Snapshot",
    "compute.googleapis.com/Address", "cloudbilling.googleapis.com/", "orgpolicy.googleapis.com/",
    "storage.googleapis.com/Object", "bigquery.googleapis.com/Table", "cloudkms.googleapis.com/CryptoKeyVersion",
    "compute.googleapis.com/InstanceSettings",
)

TYPE_LABELS = {
    "run.googleapis.com/Service": "Cloud Run",
    "run.googleapis.com/Job": "Cloud Run Job",
    "cloudfunctions.googleapis.com/Function": "Cloud Functions",
    "cloudfunctions.googleapis.com/CloudFunction": "Cloud Functions",
    "pubsub.googleapis.com/Topic": "Pub/Sub Topic",
    "pubsub.googleapis.com/Subscription": "Pub/Sub Subscription",
    "cloudscheduler.googleapis.com/Job": "Cloud Scheduler",
    "eventarc.googleapis.com/Trigger": "Eventarc",
    "sqladmin.googleapis.com/Instance": "Cloud SQL",
    "secretmanager.googleapis.com/Secret": "Secret Manager",
    "bigquery.googleapis.com/Dataset": "BigQuery",
    "storage.googleapis.com/Bucket": "Cloud Storage",
    "compute.googleapis.com/Instance": "Compute Engine",
    "iam.googleapis.com/ServiceAccount": "Service Account",
    "artifactregistry.googleapis.com/Repository": "Artifact Registry",
    "serviceusage.googleapis.com/Service": "API habilitada",
    "vpcaccess.googleapis.com/Connector": "VPC Connector",
    "container.googleapis.com/Cluster": "GKE",
    "workflows.googleapis.com/Workflow": "Workflows",
    "cloudtasks.googleapis.com/Queue": "Cloud Tasks",
    "firestore.googleapis.com/Database": "Firestore",
    "redis.googleapis.com/Instance": "Memorystore",
    "appengine.googleapis.com/Application": "App Engine",
    "cloudkms.googleapis.com/CryptoKey": "Cloud KMS",
    "cloudbuild.googleapis.com/BuildTrigger": "Cloud Build",
    "composer.googleapis.com/Environment": "Composer",
    "dataproc.googleapis.com/Cluster": "Dataproc",
    "dataproc.googleapis.com/Batch": "Dataproc",
    "dataflow.googleapis.com/Job": "Dataflow",
    "datastream.googleapis.com/Stream": "Datastream",
    "datastream.googleapis.com/ConnectionProfile": "Datastream",
    "bigquerydatatransfer.googleapis.com/TransferConfig": "Data Transfer",
    "datafusion.googleapis.com/Instance": "Data Fusion",
    "bigtableadmin.googleapis.com/Instance": "Bigtable",
    "spanner.googleapis.com/Instance": "Spanner",
    "dataplex.googleapis.com/Lake": "Dataplex",
    "aiplatform.googleapis.com/Endpoint": "Vertex AI",
}


# ============================================================ utilidades puras
def _short(path: str | None) -> str:
    return (path or "").rstrip("/").rsplit("/", 1)[-1]


def _g(d: Any, *keys: str, default: Any = None) -> Any:
    """Acceso seguro anidado: _g(data, 'spec', 'template', 'spec')."""
    for k in keys:
        if not isinstance(d, dict):
            return default
        d = d.get(k)
    return default if d is None else d


def is_default_sa(email: str | None) -> bool:
    return bool(email) and (email.endswith("-compute@developer.gserviceaccount.com")
                            or email.endswith("@appspot.gserviceaccount.com"))


def is_google_managed_sa(email: str) -> bool:
    """Agentes de servicio de Google (service-123@gcp-sa-*.iam...) — no son deuda del cliente."""
    return (email.startswith("service-") or "@gcp-sa-" in email
            or email.endswith("@cloudservices.gserviceaccount.com"))


def member_email(member: str) -> str:
    return member.split(":", 1)[-1]


@dataclass
class ArchGraph:
    nodes: dict[str, dict[str, Any]] = field(default_factory=dict)
    edges: list[dict[str, Any]] = field(default_factory=list)
    _edge_keys: set[tuple] = field(default_factory=set)

    def node(self, nid: str, ntype: str, label: str, **attrs: Any) -> str:
        if nid not in self.nodes:
            self.nodes[nid] = {"id": nid, "type": ntype, "label": label, **attrs}
        else:
            self.nodes[nid].update({k: v for k, v in attrs.items() if v is not None})
        return nid

    def edge(self, src: str, dst: str, kind: str, via: str | None = None, detail: str | None = None,
             inferred: bool = False) -> None:
        key = (src, dst, kind, via, detail)
        if src == dst or key in self._edge_keys:
            return
        self._edge_keys.add(key)
        self.edges.append({"source": src, "target": dst, "kind": kind, "via": via,
                           "detail": detail, "inferred": inferred})


# ============================================================ parser principal
class ArchitectureBuilder:
    """Convierte assets (dicts) + políticas IAM en inventario normalizado y grafo."""

    def __init__(self, project_id: str, project_number: str | None = None) -> None:
        self.project_id = project_id
        self.project_number = project_number
        self.g = ArchGraph()
        self.workloads: list[dict[str, Any]] = []
        self.databases: list[dict[str, Any]] = []
        self.other_resources: list[dict[str, Any]] = []
        self.enabled_apis: list[str] = []
        self.sa_meta: dict[str, dict[str, Any]] = {}
        self._url_index: dict[str, str] = {}      # url → node id
        self._name_index: dict[str, str] = {}     # nombre simple → node id (para referencias en env)
        self._pending_url_edges: list[tuple] = []
        self._pending_env_refs: list[tuple[str, dict[str, str]]] = []

    # ---------- helpers de nodos ----------
    def _sa(self, email: str | None) -> str | None:
        if not email:
            return None
        email = member_email(email)
        return self.g.node(f"sa:{email}", "Service Account", email, default_sa=is_default_sa(email))

    def _default_compute_sa(self) -> str | None:
        return f"{self.project_number}-compute@developer.gserviceaccount.com" if self.project_number else None

    def _index(self, nid: str, *names: str | None) -> None:
        for n in names:
            if n and len(n) >= 4:
                self._name_index.setdefault(n, nid)

    # ---------- entrada ----------
    def add_assets(self, assets: Iterable[dict[str, Any]]) -> None:
        handlers = {
            "run.googleapis.com/Service": self._cloud_run,
            "run.googleapis.com/Job": self._cloud_run_job,
            "cloudfunctions.googleapis.com/Function": self._function_v2,
            "cloudfunctions.googleapis.com/CloudFunction": self._function_v1,
            "pubsub.googleapis.com/Topic": self._topic,
            "pubsub.googleapis.com/Subscription": self._subscription,
            "cloudscheduler.googleapis.com/Job": self._scheduler,
            "eventarc.googleapis.com/Trigger": self._eventarc,
            "sqladmin.googleapis.com/Instance": self._cloud_sql,
            "secretmanager.googleapis.com/Secret": self._secret,
            "bigquery.googleapis.com/Dataset": self._dataset,
            "storage.googleapis.com/Bucket": self._bucket,
            "compute.googleapis.com/Instance": self._vm,
            "iam.googleapis.com/ServiceAccount": self._service_account,
            "serviceusage.googleapis.com/Service": self._api,
            "container.googleapis.com/Cluster": self._gke,
            "workflows.googleapis.com/Workflow": self._workflow,
            "vpcaccess.googleapis.com/Connector": self._vpc_connector,
        }
        for a in assets:
            atype, data = a.get("asset_type", ""), a.get("data") or {}
            try:
                handler = handlers.get(atype)
                if handler:
                    handler(a, data)
                elif not atype.startswith(NOISE_ASSET_PREFIXES):
                    self._generic(a, data)
            except Exception as exc:  # noqa: BLE001 — un asset raro no rompe el resto
                logger.warning("No se pudo parsear %s (%s): %s", a.get("name"), atype, exc)

    # ---------- workloads ----------
    def _workload(self, nid: str, wtype: str, name: str, *, location: str | None, sa: str | None,
                  sa_explicit: bool, url: str | None, env_plain: dict[str, str], secrets: list[str],
                  sql: list[str], vpc: str | None, ingress: str | None, min_instances: int | None,
                  labels: dict[str, str], extra: dict[str, Any] | None = None) -> None:
        if not sa and wtype in ("Cloud Run", "Cloud Functions", "Cloud Run Job"):
            sa = self._default_compute_sa()  # sin SA explícita → cuenta de Compute por defecto
        self.g.node(nid, wtype, name, location=location, url=url)
        sa_id = self._sa(sa)
        if sa_id:
            self.g.edge(nid, sa_id, "runs_as", detail="identidad de ejecución"
                        + ("" if sa_explicit else " (por defecto, no configurada explícitamente)"))
        if url:
            self._url_index[url.rstrip("/")] = nid
        for s in secrets:
            sid = self.g.node(f"secret:{_short(s)}", "Secret Manager", _short(s))
            self.g.edge(nid, sid, "reads_secret", via=sa)
        for conn in sql:
            inst = conn.split(":")[-1]
            dbid = self.g.node(f"sql:{inst}", "Cloud SQL", inst)
            self.g.edge(nid, dbid, "connects_sql", via=sa, detail=conn)
        if vpc:
            vid = self.g.node(f"vpc:{_short(vpc)}", "VPC Connector", _short(vpc))
            self.g.edge(nid, vid, "uses_vpc")
        self._pending_env_refs.append((nid, env_plain))
        self._index(nid, name)
        self.workloads.append({
            "resource_id": nid, "name": name, "service_type": wtype, "location": location, "url": url,
            "runtime_service_account": sa, "sa_explicitly_set": sa_explicit, "sa_is_default": is_default_sa(sa),
            "ingress": ingress, "min_instances": min_instances,
            # Nunca se envían VALORES de variables: sólo nombres sospechosos en texto plano.
            "plaintext_env_suspicious": sorted(k for k in env_plain if SECRET_NAME_RE.search(k)),
            "env_var_count": len(env_plain), "secrets_mounted": [_short(s) for s in secrets],
            "cloudsql_connections": sql, "vpc_connector": _short(vpc) if vpc else None,
            "tags": labels, "public_invokers": [], **(extra or {}),
        })

    @staticmethod
    def _env_from_containers(containers: list[dict[str, Any]]) -> tuple[dict[str, str], list[str]]:
        plain, secrets = {}, []
        for c in containers or []:
            for e in c.get("env", []) or []:
                ref = _g(e, "valueFrom", "secretKeyRef", "name") or _g(e, "valueSource", "secretKeyRef", "secret")
                if ref:
                    secrets.append(ref)
                elif "value" in e:
                    plain[e.get("name", "")] = str(e.get("value", ""))
        return plain, secrets

    def _cloud_run(self, a: dict[str, Any], d: dict[str, Any]) -> None:
        name = _g(d, "metadata", "name") or _short(d.get("name") or a["name"])
        if "spec" in d:  # formato Knative v1 (el que expone Cloud Asset)
            tmpl_meta = _g(d, "spec", "template", "metadata", "annotations", default={})
            svc_ann = _g(d, "metadata", "annotations", default={})
            tspec = _g(d, "spec", "template", "spec", default={})
            plain, secrets = self._env_from_containers(tspec.get("containers", []))
            secrets += [_g(v, "secret", "secretName") for v in tspec.get("volumes", []) or [] if _g(v, "secret", "secretName")]
            sa = tspec.get("serviceAccountName")
            sql = [s.strip() for s in (tmpl_meta.get("run.googleapis.com/cloudsql-instances") or "").split(",") if s.strip()]
            vpc = tmpl_meta.get("run.googleapis.com/vpc-access-connector")
            ingress = svc_ann.get("run.googleapis.com/ingress", "all")
            min_i = tmpl_meta.get("autoscaling.knative.dev/minScale")
            url = _g(d, "status", "url")
            location = _g(d, "metadata", "labels", "cloud.googleapis.com/location")
            labels = {k: v for k, v in (_g(d, "metadata", "labels", default={})).items() if not k.startswith("cloud.googleapis.com")}
        else:            # formato v2
            t = d.get("template", {})
            plain, secrets = self._env_from_containers(t.get("containers", []))
            sa = t.get("serviceAccount")
            sql = [i for v in t.get("volumes", []) or [] for i in _g(v, "cloudSqlInstance", "instances", default=[])]
            vpc = _g(t, "vpcAccess", "connector")
            ingress = d.get("ingress", "INGRESS_TRAFFIC_ALL")
            min_i = _g(t, "scaling", "minInstanceCount")
            url = d.get("uri")
            location = a.get("location")
            labels = d.get("labels", {})
        self._workload(f"run:{name}", "Cloud Run", name, location=location, sa=sa, sa_explicit=bool(sa), url=url,
                       env_plain=plain, secrets=secrets, sql=sql, vpc=vpc, ingress=ingress,
                       min_instances=int(min_i) if min_i not in (None, "") else 0, labels=labels,
                       extra={"images": [c.get("image") for c in (_g(d, "spec", "template", "spec", "containers")
                                                                    or _g(d, "template", "containers", default=[]))],
                              "resource_limits": next((_g(c, "resources", "limits") for c in (
                                  _g(d, "spec", "template", "spec", "containers") or _g(d, "template", "containers", default=[]))
                                  if _g(c, "resources", "limits")), None)})

    def _cloud_run_job(self, a: dict[str, Any], d: dict[str, Any]) -> None:
        name = _g(d, "metadata", "name") or _short(a["name"])
        tspec = _g(d, "spec", "template", "spec", "template", "spec", default={}) or _g(d, "template", "template", default={})
        plain, secrets = self._env_from_containers(tspec.get("containers", []))
        sa = tspec.get("serviceAccountName") or tspec.get("serviceAccount")
        self._workload(f"runjob:{name}", "Cloud Run Job", name, location=a.get("location"), sa=sa, sa_explicit=bool(sa),
                       url=None, env_plain=plain, secrets=secrets, sql=[], vpc=None, ingress=None, min_instances=0,
                       labels=_g(d, "metadata", "labels", default={}))

    def _function_v2(self, a: dict[str, Any], d: dict[str, Any]) -> None:
        name = _short(d.get("name") or a["name"])
        sc = d.get("serviceConfig", {})
        secrets = [s.get("secret") for s in sc.get("secretEnvironmentVariables", []) or [] if s.get("secret")]
        nid = f"fn:{name}"
        self._workload(nid, "Cloud Functions", name, location=a.get("location"), sa=sc.get("serviceAccountEmail"),
                       sa_explicit=bool(sc.get("serviceAccountEmail")), url=sc.get("uri") or d.get("url"),
                       env_plain=sc.get("environmentVariables", {}) or {}, secrets=secrets, sql=[],
                       vpc=sc.get("vpcConnector"), ingress=sc.get("ingressSettings"),
                       min_instances=sc.get("minInstanceCount", 0), labels=d.get("labels", {}),
                       extra={"runtime": _g(d, "buildConfig", "runtime"), "generation": 2})
        et = d.get("eventTrigger") or {}
        if et.get("pubsubTopic"):
            tid = self.g.node(f"topic:{_short(et['pubsubTopic'])}", "Pub/Sub Topic", _short(et["pubsubTopic"]))
            self.g.edge(tid, nid, "triggers", via=et.get("serviceAccountEmail"), detail=et.get("eventType"))
        elif et.get("eventType"):
            self.g.edge(self.g.node(f"event:{et['eventType']}", "Evento", et["eventType"]), nid, "triggers",
                        via=et.get("serviceAccountEmail"))

    def _function_v1(self, a: dict[str, Any], d: dict[str, Any]) -> None:
        name = _short(d.get("name") or a["name"])
        nid = f"fn:{name}"
        secrets = [s.get("secret") for s in d.get("secretEnvironmentVariables", []) or [] if s.get("secret")]
        self._workload(nid, "Cloud Functions", name, location=a.get("location"), sa=d.get("serviceAccountEmail"),
                       sa_explicit=True, url=_g(d, "httpsTrigger", "url"), env_plain=d.get("environmentVariables", {}) or {},
                       secrets=secrets, sql=[], vpc=d.get("vpcConnector"), ingress=d.get("ingressSettings"),
                       min_instances=d.get("minInstances", 0), labels=d.get("labels", {}),
                       extra={"runtime": d.get("runtime"), "generation": 1})
        res = _g(d, "eventTrigger", "resource")
        if res and "/topics/" in res:
            tid = self.g.node(f"topic:{_short(res)}", "Pub/Sub Topic", _short(res))
            self.g.edge(tid, nid, "triggers", detail=_g(d, "eventTrigger", "eventType"))

    def _vm(self, a: dict[str, Any], d: dict[str, Any]) -> None:
        name = d.get("name") or _short(a["name"])
        sas = d.get("serviceAccounts", []) or []
        sa = sas[0].get("email") if sas else None
        public = any(ac.get("natIP") for n in d.get("networkInterfaces", []) or [] for ac in n.get("accessConfigs", []) or [])
        self._workload(f"vm:{name}", "Compute Engine", name, location=_short(d.get("zone")), sa=sa, sa_explicit=True,
                       url=None, env_plain={}, secrets=[], sql=[], vpc=None, ingress="public-ip" if public else "internal",
                       min_instances=None, labels=d.get("labels", {}),
                       extra={"sa_scopes": sas[0].get("scopes", []) if sas else [], "status": d.get("status")})

    def _gke(self, a: dict[str, Any], d: dict[str, Any]) -> None:
        name = d.get("name") or _short(a["name"])
        sa = _g(d, "nodeConfig", "serviceAccount") or next(
            (_g(p, "config", "serviceAccount") for p in d.get("nodePools", []) or [] if _g(p, "config", "serviceAccount")), None)
        if sa == "default":
            sa = self._default_compute_sa()
        self._workload(f"gke:{name}", "GKE", name, location=d.get("location"), sa=sa, sa_explicit=bool(sa),
                       url=None, env_plain={}, secrets=[], sql=[], vpc=None,
                       ingress="private" if _g(d, "privateClusterConfig", "enablePrivateNodes") else "public-nodes",
                       min_instances=None, labels=d.get("resourceLabels", {}),
                       extra={"workload_identity": bool(_g(d, "workloadIdentityConfig", "workloadPool"))})

    def _workflow(self, a: dict[str, Any], d: dict[str, Any]) -> None:
        name = _short(d.get("name") or a["name"])
        self._workload(f"wf:{name}", "Workflows", name, location=a.get("location"), sa=d.get("serviceAccount"),
                       sa_explicit=bool(d.get("serviceAccount")), url=None, env_plain={}, secrets=[], sql=[],
                       vpc=None, ingress=None, min_instances=None, labels=d.get("labels", {}))

    # ---------- mensajería / orquestación ----------
    def _topic(self, a: dict[str, Any], d: dict[str, Any]) -> None:
        name = _short(d.get("name") or a["name"])
        nid = self.g.node(f"topic:{name}", "Pub/Sub Topic", name, labels=d.get("labels", {}))
        self._index(nid, name)

    def _subscription(self, a: dict[str, Any], d: dict[str, Any]) -> None:
        name = _short(d.get("name") or a["name"])
        sid = self.g.node(f"sub:{name}", "Pub/Sub Subscription", name)
        if d.get("topic"):
            tid = self.g.node(f"topic:{_short(d['topic'])}", "Pub/Sub Topic", _short(d["topic"]))
            self.g.edge(tid, sid, "subscribes")
        push = d.get("pushConfig") or {}
        if push.get("pushEndpoint"):
            sa = _g(push, "oidcToken", "serviceAccountEmail")
            self._sa(sa)
            self._pending_url_edges.append((sid, push["pushEndpoint"], "push", sa,
                                            None if sa else "SIN autenticación OIDC"))
        if _g(d, "bigqueryConfig", "table"):
            ds = _g(d, "bigqueryConfig", "table").split(":")[-1].split(".")[0]
            self.g.edge(sid, self.g.node(f"dataset:{ds}", "BigQuery", ds), "writes_to")
        if _g(d, "cloudStorageConfig", "bucket"):
            b = _g(d, "cloudStorageConfig", "bucket")
            self.g.edge(sid, self.g.node(f"bucket:{b}", "Cloud Storage", b), "writes_to")

    def _scheduler(self, a: dict[str, Any], d: dict[str, Any]) -> None:
        name = _short(d.get("name") or a["name"])
        jid = self.g.node(f"scheduler:{name}", "Cloud Scheduler", name, schedule=d.get("schedule"),
                          state=d.get("state"))
        http = d.get("httpTarget") or {}
        if http.get("uri"):
            sa = _g(http, "oidcToken", "serviceAccountEmail") or _g(http, "oauthToken", "serviceAccountEmail")
            self._sa(sa)
            self._pending_url_edges.append((jid, http["uri"], "invokes", sa,
                                            f"{http.get('httpMethod', 'POST')} · cron {d.get('schedule')}"))
        topic = _g(d, "pubsubTarget", "topicName")
        if topic:
            self.g.edge(jid, self.g.node(f"topic:{_short(topic)}", "Pub/Sub Topic", _short(topic)), "publishes",
                        detail=f"cron {d.get('schedule')}")

    def _eventarc(self, a: dict[str, Any], d: dict[str, Any]) -> None:
        name = _short(d.get("name") or a["name"])
        eid = self.g.node(f"eventarc:{name}", "Eventarc", name,
                          filters=[f"{f.get('attribute')}={f.get('value')}" for f in d.get("eventFilters", []) or []])
        sa = d.get("serviceAccount")
        self._sa(sa)
        dest = _g(d, "destination", "cloudRun", "service") or _g(d, "destination", "cloudRunService", "service")
        if dest:
            self.g.edge(eid, self.g.node(f"run:{dest}", "Cloud Run", dest), "triggers", via=sa)
        fn = _g(d, "destination", "cloudFunction")
        if fn:
            self.g.edge(eid, self.g.node(f"fn:{_short(fn)}", "Cloud Functions", _short(fn)), "triggers", via=sa)
        topic = _g(d, "transport", "pubsub", "topic")
        if topic:
            self.g.edge(self.g.node(f"topic:{_short(topic)}", "Pub/Sub Topic", _short(topic)), eid, "transport")

    # ---------- datos ----------
    def _cloud_sql(self, a: dict[str, Any], d: dict[str, Any]) -> None:
        name = d.get("name") or _short(a["name"])
        ipc = _g(d, "settings", "ipConfiguration", default={})
        nets = [n.get("value") for n in ipc.get("authorizedNetworks", []) or []]
        nid = self.g.node(f"sql:{name}", "Cloud SQL", name, engine=d.get("databaseVersion"))
        self._index(nid, name, d.get("connectionName"))
        self.databases.append({
            "resource_id": nid, "name": name, "engine": d.get("databaseVersion"), "tier": _g(d, "settings", "tier"),
            "region": d.get("region"), "connection_name": d.get("connectionName"),
            "public_ip": bool(ipc.get("ipv4Enabled")), "authorized_networks": nets,
            "open_to_internet": "0.0.0.0/0" in nets,
            "ssl_mode": ipc.get("sslMode") or ("REQUIRED" if ipc.get("requireSsl") else "ALLOW_UNENCRYPTED"),
            "backups_enabled": bool(_g(d, "settings", "backupConfiguration", "enabled")),
            "high_availability": _g(d, "settings", "availabilityType") == "REGIONAL",
            "deletion_protection": bool(_g(d, "settings", "deletionProtectionEnabled")),
            "tags": _g(d, "settings", "userLabels", default={}),
        })
        if ipc.get("ipv4Enabled") and nets:
            self.g.edge(self._internet(), nid, "public_access", detail="redes autorizadas: " + ", ".join(nets))

    def _secret(self, a: dict[str, Any], d: dict[str, Any]) -> None:
        name = _short(d.get("name") or a["name"])
        nid = self.g.node(f"secret:{name}", "Secret Manager", name,
                          rotation=bool(d.get("rotation")), labels=d.get("labels", {}))
        self._index(nid, name)

    def _dataset(self, a: dict[str, Any], d: dict[str, Any]) -> None:
        ds = _g(d, "datasetReference", "datasetId") or _short(a["name"])
        nid = self.g.node(f"dataset:{ds}", "BigQuery", ds, location=d.get("location"))
        self._index(nid, ds)
        for acc in d.get("access", []) or []:
            who = acc.get("userByEmail") or acc.get("iamMember") or acc.get("groupByEmail") or acc.get("specialGroup")
            if who in ("allAuthenticatedUsers", "allUsers"):
                self.g.edge(self._internet(), nid, "public_access", detail=f"{who} → {acc.get('role')}")
            elif who and who.endswith(".gserviceaccount.com"):
                self.g.edge(self._sa(who), nid, "has_role", detail=acc.get("role"))

    def _bucket(self, a: dict[str, Any], d: dict[str, Any]) -> None:
        name = d.get("name") or _short(a["name"])
        nid = self.g.node(f"bucket:{name}", "Cloud Storage", name, location=d.get("location"))
        self._index(nid, name)

    def _service_account(self, a: dict[str, Any], d: dict[str, Any]) -> None:
        email = d.get("email") or _short(a["name"])
        self.sa_meta[email] = {"display_name": d.get("displayName"), "disabled": bool(d.get("disabled")),
                               "description": d.get("description")}
        self._sa(email)

    def _vpc_connector(self, a: dict[str, Any], d: dict[str, Any]) -> None:
        name = _short(d.get("name") or a["name"])
        self.g.node(f"vpc:{name}", "VPC Connector", name, network=d.get("network"))

    def _api(self, a: dict[str, Any], d: dict[str, Any]) -> None:
        if d.get("state") in (None, "ENABLED"):
            self.enabled_apis.append(_short(d.get("name") or a["name"]))
        if not self.project_number:
            for text in (d.get("parent"), d.get("name"), a.get("name")):
                m = re.search(r"projects/(\d+)(?:/|$)", text or "")
                if m:
                    self.project_number = m.group(1)
                    break

    def _generic(self, a: dict[str, Any], d: dict[str, Any]) -> None:
        label = TYPE_LABELS.get(a["asset_type"], a["asset_type"].split("/")[-1])
        name = d.get("displayName") or d.get("name") or _short(a["name"])
        name = _short(name)
        nid = self.g.node(f"res:{a['asset_type'].split('.')[0]}:{name}", label, name)
        self._index(nid, name)
        self.other_resources.append({"resource_id": nid, "type": label, "asset_type": a["asset_type"],
                                     "location": a.get("location"), "tags": d.get("labels", {}) or {}})

    def _internet(self) -> str:
        return self.g.node("internet", "Internet", "🌐 Internet (público)")

    # ---------- IAM ----------
    def add_resource_iam(self, policies: Iterable[dict[str, Any]]) -> None:
        """Bindings a nivel recurso (bucket, topic, secret, servicio Cloud Run...)."""
        for p in policies:
            atype, rname = p.get("asset_type", ""), p.get("name", "")
            if atype.startswith("cloudresourcemanager.googleapis.com/Project"):
                continue  # la política de proyecto se trata aparte
            target = self._node_for_asset(atype, rname)
            for b in p.get("bindings", []):
                for m in b.get("members", []):
                    if m in PUBLIC_MEMBERS:
                        self.g.edge(self._internet(), target, "public_access", detail=f"{m} → {b['role']}")
                        for w in self.workloads:
                            if w["resource_id"] == target:
                                w["public_invokers"].append(f"{m} → {b['role']}")
                    elif m.startswith("serviceAccount:"):
                        self.g.edge(self._sa(m), target, "has_role", detail=b["role"])
                    elif m.startswith(("user:", "group:")):
                        uid = self.g.node(f"principal:{member_email(m)}", "Usuario/Grupo", member_email(m))
                        self.g.edge(uid, target, "has_role", detail=b["role"])

    def _node_for_asset(self, atype: str, rname: str) -> str:
        n = _short(rname)
        prefix = {"run.googleapis.com/Service": "run", "cloudfunctions.googleapis.com/Function": "fn",
                  "cloudfunctions.googleapis.com/CloudFunction": "fn", "pubsub.googleapis.com/Topic": "topic",
                  "pubsub.googleapis.com/Subscription": "sub", "secretmanager.googleapis.com/Secret": "secret",
                  "storage.googleapis.com/Bucket": "bucket", "bigquery.googleapis.com/Dataset": "dataset",
                  "iam.googleapis.com/ServiceAccount": "sa", "sqladmin.googleapis.com/Instance": "sql",
                  "compute.googleapis.com/Instance": "vm"}.get(atype)
        if prefix == "sa":
            return self._sa(n)  # type: ignore[return-value]
        nid = f"{prefix}:{n}" if prefix else f"res:{atype.split('.')[0]}:{n}"
        return self.g.node(nid, TYPE_LABELS.get(atype, atype.split("/")[-1]), n)

    # ---------- cierre ----------
    def _resolve_url(self, url: str) -> str | None:
        base = re.match(r"^(https?://[^/]+)", url or "")
        if not base:
            return None
        for known, nid in self._url_index.items():
            if known.startswith(base.group(1)):
                return nid
        host = base.group(1).split("//")[1]
        m = re.match(r"^([a-z0-9-]+?)-[a-z0-9]{8,12}-[a-z]{2}\.a\.run\.app$", host) or re.match(r"^([a-z0-9-]+)-\d+\.[a-z0-9-]+\.run\.app$", host)
        return f"run:{m.group(1)}" if m else None

    def build(self, project_iam: list[dict[str, Any]]) -> dict[str, Any]:
        # URLs (scheduler/push) → workload destino
        for src, url, kind, sa, detail in self._pending_url_edges:
            dst = self._resolve_url(url) or self.g.node(f"url:{url}", "Endpoint externo", url)
            self.g.edge(src, dst, kind, via=sa, detail=detail)
        # Variables de entorno que mencionan recursos conocidos → relación INFERIDA
        for nid, env in self._pending_env_refs:
            for key, val in env.items():
                for rname, rid in self._name_index.items():
                    if rid != nid and rname in val:
                        self.g.edge(nid, rid, "references", via=None, detail=f"env {key}", inferred=True)

        identities = self._identities(project_iam)
        # Enriquecer workloads con los roles de su SA
        idx = {i["resource_id"]: i for i in identities}
        for w in self.workloads:
            ident = idx.get(w["runtime_service_account"] or "")
            w["sa_project_roles"] = ident["project_roles"] if ident else []
            w["sa_overprivileged"] = bool(ident and ident["overprivileged"])
        # Permisos efectivos en cada arista con identidad
        for e in self.g.edges:
            email = e["via"] or (e["target"][3:] if e["kind"] == "runs_as" else None)
            ident = idx.get(email or "")
            if ident:
                grants = [g["role"] for g in ident["resource_grants"] if g["resource"] == e["target"]]
                e["permissions"] = sorted(set(ident["project_roles"]) | set(grants))

        return {
            "workloads": self.workloads,
            "databases": self.databases,
            "service_identities": identities,
            "other_resources": self.other_resources,
            "enabled_apis": sorted(set(self.enabled_apis)),
            "architecture": {"nodes": list(self.g.nodes.values()), "edges": self.g.edges,
                             "project_number": self.project_number},
        }

    def _identities(self, project_iam: list[dict[str, Any]]) -> list[dict[str, Any]]:
        proj_roles = {r["resource_id"]: r.get("attached_policies", []) for r in project_iam or []}
        out = []
        for nid, node in self.g.nodes.items():
            if node["type"] != "Service Account":
                continue
            email = node["label"]
            roles = [r for r in proj_roles.get(f"serviceAccount:{email}", []) if isinstance(r, str)]
            used_by = [e["source"] for e in self.g.edges if e["kind"] == "runs_as" and e["target"] == nid]
            used_by += [e["source"] for e in self.g.edges if e["via"] == email and e["kind"] != "runs_as"]
            grants = [{"resource": e["target"], "role": e["detail"]} for e in self.g.edges
                      if e["source"] == nid and e["kind"] == "has_role"]
            broad = sorted(set(roles) & BROAD_ROLES)
            esc = sorted(set(roles) & PRIV_ESC_ROLES)
            admin = sorted(r for r in roles if r.endswith(".admin") or r.endswith("Admin"))
            key_info = next((r for r in project_iam or [] if r["resource_id"] == f"serviceAccount:{email}"), {})
            out.append({
                "resource_id": email, "is_default_sa": is_default_sa(email),
                "google_managed": is_google_managed_sa(email),
                "used_by": sorted(set(used_by)), "project_roles": roles, "resource_grants": grants,
                "broad_roles": broad, "privilege_escalation_roles": esc, "admin_roles": admin,
                "overprivileged": bool(broad or esc or len(admin) >= 2) and not is_google_managed_sa(email),
                "user_managed_keys": key_info.get("user_managed_keys"),
                "oldest_key_age_days": key_info.get("access_key_age_days"),
                **self.sa_meta.get(email, {}),
            })
        return out


# ============================================================ recolección (red)
class GCPArchitectureCollector:
    """Llama a Cloud Asset, Billing y (opcional) BigQuery del export de facturación."""

    def __init__(self, credentials: Any, project_id: str, billing_export_table: str | None = None) -> None:
        self.credentials = credentials
        self.project_id = project_id
        self.billing_export_table = billing_export_table

    def list_assets(self, content: str) -> list[dict[str, Any]]:
        from google.cloud import asset_v1
        from google.protobuf import json_format

        client = asset_v1.AssetServiceClient(credentials=self.credentials)
        ctype = asset_v1.ContentType.RESOURCE if content == "resource" else asset_v1.ContentType.IAM_POLICY
        out = []
        for asset in client.list_assets(request={"parent": f"projects/{self.project_id}", "content_type": ctype,
                                                 "page_size": 1000}, timeout=90):
            pb = asset_v1.Asset.pb(asset)
            if content == "resource":
                out.append({"name": asset.name, "asset_type": asset.asset_type,
                            "location": asset.resource.location or None,
                            "data": json_format.MessageToDict(pb.resource.data)})
            else:
                out.append({"name": asset.name, "asset_type": asset.asset_type,
                            "bindings": [{"role": b.role, "members": list(b.members)} for b in asset.iam_policy.bindings]})
        return out

    def billing(self, project_number: str | None) -> dict[str, Any]:
        from google.cloud import billing_v1

        info = billing_v1.CloudBillingClient(credentials=self.credentials).get_project_billing_info(
            name=f"projects/{self.project_id}", timeout=30)
        result: dict[str, Any] = {"resource_id": f"billing:{self.project_id}",
                                  "billing_account": info.billing_account_name or None,
                                  "billing_enabled": info.billing_enabled, "budgets": None, "has_budget": None,
                                  "budget_error": None, "cost_by_service_30d": None, "total_cost_30d_usd": None}
        if info.billing_account_name:
            try:
                from google.cloud.billing import budgets_v1

                budgets = []
                for b in budgets_v1.BudgetServiceClient(credentials=self.credentials).list_budgets(
                        parent=info.billing_account_name, timeout=30):
                    projects = list(b.budget_filter.projects)
                    applies = not projects or any(p.endswith(f"/{project_number}") or p.endswith(f"/{self.project_id}")
                                                  for p in projects)
                    if applies:
                        amount = b.amount.specified_amount
                        budgets.append({"name": b.display_name,
                                        "amount": float(amount.units or 0) + amount.nanos / 1e9 if amount else None,
                                        "currency": amount.currency_code if amount else None,
                                        "thresholds": [t.threshold_percent for t in b.threshold_rules]})
                result["budgets"], result["has_budget"] = budgets, bool(budgets)
            except Exception as exc:  # noqa: BLE001
                result["budget_error"] = f"{type(exc).__name__}: {str(exc)[:200]}"
        return result

    def billing_costs(self) -> list[dict[str, Any]]:
        from google.cloud import bigquery

        table = (self.billing_export_table or "").strip().strip("`")
        if not BILLING_TABLE_RE.match(table):
            raise ValueError("La tabla de export debe tener el formato proyecto.dataset.tabla")
        client = bigquery.Client(project=table.split(".")[0], credentials=self.credentials)
        sql = f"""
            SELECT service.description AS service,
                   ROUND(SUM(cost) + SUM(IFNULL((SELECT SUM(c.amount) FROM UNNEST(credits) c), 0)), 2) AS net_cost,
                   ANY_VALUE(currency) AS currency
            FROM `{table}`
            WHERE project.id = @project
              AND usage_start_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
            GROUP BY service ORDER BY net_cost DESC LIMIT 50"""
        job = client.query(sql, job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("project", "STRING", self.project_id)]))
        return [dict(r) for r in job.result(timeout=90)]

    def collect(self, project_iam: list[dict[str, Any]], describe_error: Any) -> tuple[dict[str, Any], list[str], list[str]]:
        errors, ok = [], []
        resources, policies = [], []
        try:
            resources = self.list_assets("resource")
            ok.append("asset_inventory")
        except Exception as exc:  # noqa: BLE001
            errors.append(describe_error("asset_inventory", exc))
        try:
            policies = self.list_assets("iam")
            ok.append("asset_iam")
        except Exception as exc:  # noqa: BLE001
            errors.append(describe_error("asset_iam", exc))

        builder = ArchitectureBuilder(self.project_id)
        builder.add_assets(sorted(resources, key=lambda a: a["asset_type"] != "serviceusage.googleapis.com/Service"))
        builder.add_resource_iam(policies)
        data = builder.build(project_iam)

        try:
            data["billing"] = self.billing(builder.project_number)
            ok.append("billing")
        except Exception as exc:  # noqa: BLE001
            errors.append(describe_error("billing", exc))
            data["billing"] = {"resource_id": f"billing:{self.project_id}", "billing_enabled": None, "has_budget": None}
        if self.billing_export_table:
            try:
                costs = self.billing_costs()
                data["billing"]["cost_by_service_30d"] = costs
                data["billing"]["total_cost_30d_usd"] = round(sum(c["net_cost"] or 0 for c in costs), 2)
                ok.append("billing_export")
            except Exception as exc:  # noqa: BLE001
                errors.append(describe_error("billing_export", exc))
        data["asset_counts"] = _count_types(resources)
        data["_assets"] = resources  # lo consume el colector de datos; discovery lo elimina antes de guardar
        data["architecture"]["collected_at"] = datetime.now(timezone.utc).isoformat()
        return data, errors, ok


def _count_types(resources: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for r in resources:
        label = TYPE_LABELS.get(r["asset_type"], r["asset_type"])
        counts[label] = counts.get(label, 0) + 1
    return dict(sorted(counts.items(), key=lambda kv: -kv[1]))
