"""Tests del grafo de arquitectura con assets con el formato real de Cloud Asset Inventory."""
from __future__ import annotations

from backend.agent_graph.nodes.gcp_architecture import ArchitectureBuilder

SA_API = "api-runtime@proj-1.iam.gserviceaccount.com"
SA_SCHED = "scheduler-invoker@proj-1.iam.gserviceaccount.com"

ASSETS = [
    {"name": "//serviceusage.googleapis.com/projects/123456/services/run.googleapis.com",
     "asset_type": "serviceusage.googleapis.com/Service",
     "data": {"name": "projects/123456/services/run.googleapis.com", "state": "ENABLED", "parent": "projects/123456"}},
    {"name": "//run.googleapis.com/projects/proj-1/locations/us-central1/services/api", "asset_type": "run.googleapis.com/Service",
     "data": {"metadata": {"name": "api", "labels": {"cloud.googleapis.com/location": "us-central1", "owner": "x"},
                           "annotations": {"run.googleapis.com/ingress": "all"}},
              "spec": {"template": {"metadata": {"annotations": {
                  "run.googleapis.com/cloudsql-instances": "proj-1:us-central1:main-db",
                  "autoscaling.knative.dev/minScale": "1"}},
                  "spec": {"serviceAccountName": SA_API, "containers": [{"image": "gcr.io/x/api", "env": [
                      {"name": "BUCKET", "value": "mantum-apu-comparativos-452fd151"},
                      {"name": "DB_PASSWORD", "value": "hunter2"},
                      {"name": "API_TOKEN", "valueFrom": {"secretKeyRef": {"name": "api-token", "key": "latest"}}}]}]}}},
              "status": {"url": "https://api-abc123xyz-uc.a.run.app"}}},
    {"name": "//run.googleapis.com/projects/proj-1/locations/us-central1/services/worker", "asset_type": "run.googleapis.com/Service",
     "data": {"metadata": {"name": "worker"}, "spec": {"template": {"spec": {"containers": [{"image": "w"}]}}},
              "status": {"url": "https://worker-abc123xyz-uc.a.run.app"}}},
    {"name": "//cloudscheduler.googleapis.com/projects/proj-1/locations/us-central1/jobs/nightly",
     "asset_type": "cloudscheduler.googleapis.com/Job",
     "data": {"name": "projects/proj-1/locations/us-central1/jobs/nightly", "schedule": "0 2 * * *",
              "httpTarget": {"uri": "https://api-abc123xyz-uc.a.run.app/tasks/run", "httpMethod": "POST",
                             "oidcToken": {"serviceAccountEmail": SA_SCHED}}}},
    {"name": "//pubsub.googleapis.com/projects/proj-1/topics/events", "asset_type": "pubsub.googleapis.com/Topic",
     "data": {"name": "projects/proj-1/topics/events"}},
    {"name": "//pubsub.googleapis.com/projects/proj-1/subscriptions/events-push", "asset_type": "pubsub.googleapis.com/Subscription",
     "data": {"name": "projects/proj-1/subscriptions/events-push", "topic": "projects/proj-1/topics/events",
              "pushConfig": {"pushEndpoint": "https://worker-abc123xyz-uc.a.run.app/"}}},
    {"name": "//sqladmin.googleapis.com/projects/proj-1/instances/main-db", "asset_type": "sqladmin.googleapis.com/Instance",
     "data": {"name": "main-db", "databaseVersion": "POSTGRES_15", "connectionName": "proj-1:us-central1:main-db",
              "settings": {"ipConfiguration": {"ipv4Enabled": True, "authorizedNetworks": [{"value": "0.0.0.0/0"}]},
                           "backupConfiguration": {"enabled": False}}}},
    {"name": "//storage.googleapis.com/mantum-apu-comparativos-452fd151", "asset_type": "storage.googleapis.com/Bucket",
     "data": {"name": "mantum-apu-comparativos-452fd151"}},
    {"name": "//iam.googleapis.com/projects/proj-1/serviceAccounts/x", "asset_type": "iam.googleapis.com/ServiceAccount",
     "data": {"email": SA_API, "displayName": "API runtime"}},
    {"name": "//compute.googleapis.com/projects/proj-1/global/routes/default", "asset_type": "compute.googleapis.com/Route", "data": {}},
]
IAM = [
    {"name": "//run.googleapis.com/projects/proj-1/locations/us-central1/services/api", "asset_type": "run.googleapis.com/Service",
     "bindings": [{"role": "roles/run.invoker", "members": ["allUsers", f"serviceAccount:{SA_SCHED}"]}]},
    {"name": "//storage.googleapis.com/mantum-apu-comparativos-452fd151", "asset_type": "storage.googleapis.com/Bucket",
     "bindings": [{"role": "roles/storage.objectAdmin", "members": [f"serviceAccount:{SA_API}"]}]},
]
PROJECT_IAM = [
    {"resource_id": f"serviceAccount:{SA_API}", "attached_policies": ["roles/editor", "roles/cloudsql.client"]},
    {"resource_id": "serviceAccount:123456-compute@developer.gserviceaccount.com", "attached_policies": ["roles/editor"]},
]


def _build():
    b = ArchitectureBuilder("proj-1")
    b.add_assets(ASSETS)
    b.add_resource_iam(IAM)
    return b.build(PROJECT_IAM)


def _edge(out, src, dst, kind):
    return next((e for e in out["architecture"]["edges"]
                 if e["source"] == src and e["target"] == dst and e["kind"] == kind), None)


def test_relationships():
    out = _build()
    assert out["architecture"]["project_number"] == "123456"
    assert _edge(out, "run:api", f"sa:{SA_API}", "runs_as")
    inv = _edge(out, "scheduler:nightly", "run:api", "invokes")
    assert inv and inv["via"] == SA_SCHED                       # URL resuelta al servicio
    push = _edge(out, "sub:events-push", "run:worker", "push")
    assert push and "SIN autenticación" in push["detail"]
    assert _edge(out, "topic:events", "sub:events-push", "subscribes")
    sql = _edge(out, "run:api", "sql:main-db", "connects_sql")
    assert sql and "roles/cloudsql.client" in sql["permissions"]   # permisos efectivos de la identidad
    assert _edge(out, "run:api", "secret:api-token", "reads_secret")
    ref = _edge(out, "run:api", "bucket:mantum-apu-comparativos-452fd151", "references")
    assert ref and ref["inferred"]
    assert _edge(out, f"sa:{SA_API}", "bucket:mantum-apu-comparativos-452fd151", "has_role")
    assert _edge(out, "internet", "run:api", "public_access")
    assert _edge(out, "internet", "sql:main-db", "public_access")
    assert not any(n["id"].endswith("default") for n in out["architecture"]["nodes"])  # ruido filtrado


def test_workloads_and_identities():
    out = _build()
    api = next(w for w in out["workloads"] if w["resource_id"] == "run:api")
    assert api["plaintext_env_suspicious"] == ["DB_PASSWORD"]
    assert "hunter2" not in str(out)                                # los VALORES nunca salen
    assert api["public_invokers"] and api["sa_overprivileged"] and api["min_instances"] == 1
    worker = next(w for w in out["workloads"] if w["resource_id"] == "run:worker")
    assert worker["sa_is_default"] and not worker["sa_explicitly_set"]
    ident = {i["resource_id"]: i for i in out["service_identities"]}
    assert ident[SA_API]["overprivileged"] and ident[SA_API]["broad_roles"] == ["roles/editor"]
    assert "run:worker" in ident["123456-compute@developer.gserviceaccount.com"]["used_by"]
    db = out["databases"][0]
    assert db["open_to_internet"] and not db["backups_enabled"]


def test_analyzers_accept_architecture():
    import json

    from backend.agent_graph.nodes.cost import CostAnalyzer
    from backend.agent_graph.nodes.governance import GovernanceAnalyzer
    from backend.agent_graph.nodes.security import SecurityAnalyzer
    from backend.core.kpi_engine import compute_kpis

    raw = {**_build(), "billing": {"resource_id": "billing:proj-1", "billing_enabled": True, "has_budget": False}}
    for a in (SecurityAnalyzer(), GovernanceAnalyzer(), CostAnalyzer()):
        data = a.select_data(raw)
        json.dumps(data, default=str)
        assert "run:api" in a._known_ids(data)
    k = compute_kpis(raw, [{"rule": "DEFAULT_SA_IN_USE", "resource_id": "run:worker", "category": "security",
                            "severity": "HIGH"}])
    assert k["security"]["pct_workloads_default_sa"] == 50.0 and k["cost"]["has_budget"] is False
