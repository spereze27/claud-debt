"""Tests del discovery real de GCP con clientes SDK simulados (sin red)."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from types import SimpleNamespace as NS

import pytest
from google.api_core import exceptions as gexc

from backend.agent_graph.nodes import discovery as disc
from backend.agent_graph.nodes.cost import CostAnalyzer
from backend.agent_graph.nodes.governance import GovernanceAnalyzer
from backend.agent_graph.nodes.security import SecurityAnalyzer


@pytest.fixture(autouse=True)
def real_mode(monkeypatch):
    monkeypatch.setenv("DISCOVERY_MODE", "real")


@pytest.fixture
def provider(monkeypatch, tmp_path):
    """GCPDiscoveryProvider con credenciales falsas."""
    f = tmp_path / "sa.json"
    f.write_text(json.dumps({"type": "service_account"}))
    monkeypatch.setattr("google.oauth2.service_account.Credentials.from_service_account_file",
                        lambda path, scopes: object())
    return disc.GCPDiscoveryProvider(str(f), "proj-1"), str(f)


def _fake_sdk(monkeypatch, storage_error: Exception | None = None):
    from google.cloud import compute_v1, iam_admin_v1, monitoring_v3, resourcemanager_v3, storage
    from backend.agent_graph.nodes import gcp_architecture as ga

    from backend.agent_graph.nodes import gcp_data

    monkeypatch.setattr(ga.GCPArchitectureCollector, "list_assets", lambda self, content: [])
    monkeypatch.setattr(gcp_data.GCPDataCollector, "collect",
                        lambda self, describe: ({"project_id": self.project_id, "clusters": []}, [], ["bigquery_jobs"]))
    monkeypatch.setattr(ga.GCPArchitectureCollector, "billing",
                        lambda self, n: {"resource_id": "billing:proj-1", "billing_enabled": True, "has_budget": False})

    policy = NS(bindings=[NS(role="roles/owner", members=["user:ana@x.com", "serviceAccount:ci@proj-1.iam.gserviceaccount.com"], condition=None),
                          NS(role="roles/viewer", members=["group:devs@x.com"], condition=None)])
    monkeypatch.setattr(resourcemanager_v3, "ProjectsClient", lambda credentials: NS(get_iam_policy=lambda request: policy))

    bucket = NS(name="datos-prod", location="US", storage_class="STANDARD", labels={"owner": "ana"},
                default_kms_key_name=None, versioning_enabled=False, retention_period=None,
                time_created=datetime(2024, 1, 1, tzinfo=timezone.utc),
                iam_configuration=NS(public_access_prevention="inherited", uniform_bucket_level_access_enabled=True),
                get_iam_policy=lambda requested_policy_version: NS(bindings=[{"role": "roles/storage.objectViewer", "members": {"allUsers"}}]))

    def storage_client(project, credentials):
        if storage_error:
            raise storage_error
        return NS(list_buckets=lambda: [bucket])
    monkeypatch.setattr(storage, "Client", storage_client)

    fw = NS(name="allow-ssh", direction="INGRESS", disabled=False, network="x/global/networks/default",
            source_ranges=["0.0.0.0/0"], target_tags=["ssh"], target_service_accounts=[],
            allowed=[NS(ports=["22"], I_p_protocol="tcp")])
    inst = NS(name="vm-1", id=111, machine_type="x/machineTypes/e2-medium", status="RUNNING",
              network_interfaces=[NS(network="x/global/networks/default", access_configs=[NS(nat_i_p="34.1.2.3")])],
              tags=NS(items=["ssh"]), service_accounts=[], deletion_protection=False, labels={},
              creation_timestamp="2024-01-01T00:00:00Z")
    disk_orphan = NS(name="disk-huerfano", users=[], last_detach_timestamp="2025-01-01T00:00:00Z",
                     creation_timestamp="2024-01-01T00:00:00Z", type_="x/diskTypes/pd-ssd", size_gb=100,
                     disk_encryption_key=None, labels={})
    addr = NS(name="ip-1", address="35.0.0.1", address_type="EXTERNAL", status="RESERVED", users=[])
    snap = NS(name="snap-1", source_disk="x/disks/borrado", creation_timestamp="2024-01-01T00:00:00Z",
              storage_bytes=10 * 1024 ** 3, disk_size_gb=100)
    monkeypatch.setattr(compute_v1, "FirewallsClient", lambda credentials: NS(list=lambda project: [fw]))
    monkeypatch.setattr(compute_v1, "InstancesClient", lambda credentials: NS(
        aggregated_list=lambda request: [("zones/us-central1-a", NS(instances=[inst]))]))
    monkeypatch.setattr(compute_v1, "DisksClient", lambda credentials: NS(
        aggregated_list=lambda request: [("zones/us-central1-a", NS(disks=[disk_orphan]))]))
    monkeypatch.setattr(compute_v1, "AddressesClient", lambda credentials: NS(
        aggregated_list=lambda request: [("regions/us-central1", NS(addresses=[addr]))]))
    monkeypatch.setattr(compute_v1, "SnapshotsClient", lambda credentials: NS(list=lambda project: [snap]))
    monkeypatch.setattr(iam_admin_v1, "IAMClient", lambda credentials: NS(list_service_accounts=lambda request: []))
    series = NS(resource=NS(labels={"instance_id": "111"}), points=[NS(value=NS(double_value=0.02))])
    monkeypatch.setattr(monitoring_v3, "MetricServiceClient", lambda credentials: NS(list_time_series=lambda request: [series]))


def test_folder_scope_merges_projects(monkeypatch, provider):
    _fake_sdk(monkeypatch)
    _, path = provider
    monkeypatch.setattr(disc, "list_projects", lambda cred, scope, target, n: [
        {"project_id": "proj-1", "folder_path": "123 / ventas", "display_name": "a"},
        {"project_id": "proj-2", "folder_path": "123 / finanzas", "display_name": "b"}])
    out = disc.discovery_node({"provider": "GCP", "scope": "folder", "target_id": "123", "credentials_path": path})
    raw = out["raw_infra_data"]
    assert [p["project_id"] for p in raw["projects"]] == ["proj-1", "proj-2"]
    assert raw["metadata"]["projects_analyzed"] == 2
    ids = [i["resource_id"] for i in raw["compute_instances"]]
    assert ids == ["proj-1/vm-1", "proj-2/vm-1"]                          # sin colisiones entre proyectos
    assert raw["data_platform"]["cost_breakdown"]["total_usd"] >= 0


def test_full_collection_is_compatible_with_analyzers(monkeypatch, provider):
    _fake_sdk(monkeypatch)
    _, path = provider
    out = disc.discovery_node({"provider": "GCP", "scope": "project", "target_id": "proj-1", "credentials_path": path})
    raw = out["raw_infra_data"]
    assert out["discovery_status"] == "complete" and not out["errors"]

    owner = next(r for r in raw["iam_roles"] if r["resource_id"] == "user:ana@x.com")
    assert owner["attached_policies"] == ["roles/owner"]
    b = raw["storage_buckets"][0]
    assert b["public_access"] is True and b["encryption"] == "PROVIDER_MANAGED"
    vm = raw["compute_instances"][0]
    assert vm["public_ip"] == "34.1.2.3" and vm["firewall_ingress"][0]["source"] == "0.0.0.0/0"
    assert vm["avg_cpu_30d_pct"] == 2.0
    d = raw["block_volumes"][0]
    assert d["attached_to"] is None and d["monthly_cost_usd"] == 17.0
    assert raw["static_ips"][0]["associated_with"] is None
    assert raw["snapshots"][0]["source_volume_exists"] is False

    # Los analizadores existentes deben poder seleccionar sus datos sin KeyError.
    for analyzer in (SecurityAnalyzer(), GovernanceAnalyzer(), CostAnalyzer()):
        json.dumps(analyzer.select_data(raw), default=str)


def test_permission_denied_is_partial_not_crash(monkeypatch, provider):
    _fake_sdk(monkeypatch, storage_error=gexc.Forbidden("storage.buckets.list denied"))
    _, path = provider
    out = disc.discovery_node({"provider": "GCP", "scope": "project", "target_id": "proj-1", "credentials_path": path})
    assert out["discovery_status"] == "partial"
    assert out["raw_infra_data"]["storage_buckets"] == []
    assert any("PERMISO DENEGADO en 'storage'" in e and "roles/" in e for e in out["errors"])
    assert out["raw_infra_data"]["iam_roles"]  # el resto sí se recolectó


@pytest.mark.parametrize("state, fragment", [
    ({"provider": "AWS", "scope": "project", "target_id": "p"}, "sólo está implementado para GCP"),
    ({"provider": "GCP", "scope": "project", "target_id": "p"}, "No se proporcionó"),
    ({"provider": "GCP", "scope": "project", "target_id": "p", "credentials_path": "/no/existe.json"}, "No se encontró"),
])
def test_fatal_errors_return_failed_state(state, fragment):
    out = disc.discovery_node(state)
    assert out["discovery_status"] == "failed" and fragment in out["errors"][0]


def test_graph_survives_failed_discovery(monkeypatch):
    """Con discovery fallido el grafo termina y el informe refleja el problema."""
    from backend.agent_graph import graph as graph_mod
    from backend.agent_graph.nodes import report

    monkeypatch.setattr(report, "get_llm_from_config", lambda cfg: (_ for _ in ()).throw(RuntimeError("sin LLM")))
    graph_mod.get_compiled_graph.cache_clear()
    out = graph_mod.get_compiled_graph().invoke(
        {"provider": "GCP", "scope": "project", "target_id": "p1", "findings": [], "errors": []},
        {"configurable": {"llm_api_key": "x"}})
    assert out["discovery_status"] == "failed"
    assert "Problemas de cobertura" in out["final_report"]


def test_missing_sdk_gives_clear_message(monkeypatch, tmp_path):
    monkeypatch.setattr(disc, "missing_gcp_modules", lambda: ["google.api_core"])
    out = disc.discovery_node({"provider": "GCP", "scope": "project", "target_id": "p", "credentials_path": "x"})
    assert out["discovery_status"] == "failed" and "pip install" in out["errors"][0]


def test_describe_error_without_sdk():
    assert "FALTAN LIBRERÍAS" in disc._describe_error("iam", ModuleNotFoundError("No module named 'google.api_core'"))
