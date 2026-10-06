"""Tests end-to-end del grafo con un LLM falso (sin coste ni API Key real)."""
from __future__ import annotations

from typing import Any

import pytest
from langchain_core.messages import AIMessage

from backend.agent_graph import graph as graph_mod
from backend.agent_graph.nodes import base, report
from backend.agent_graph.nodes.discovery_mock import MockDiscoveryProvider
from backend.core.kpi_engine import compute_kpis


class FakeLLM:
    """Simula with_structured_output devolviendo hallazgos reales + uno alucinado."""

    def __init__(self) -> None:
        self._schema = None

    def with_structured_output(self, schema: Any) -> "FakeLLM":
        self._schema = schema
        return self

    def invoke(self, messages: list) -> Any:
        if self._schema is None:
            return AIMessage(content="# Informe\nOK")
        text = messages[1].content
        rid, rule, cat_waste = None, None, 0.0
        if "iam_roles" in text and "firewall_ingress" in text:
            rid, rule = "iam-role-000", "IAM_ADMIN_ROLE"
        elif "encryption" in text:
            rid, rule = text.split('"resource_id": "')[1].split('"')[0], "UNENCRYPTED_BUCKET"
        else:
            rid, rule, cat_waste = text.split('"resource_id": "')[1].split('"')[0], "IDLE_INSTANCE", 50.0
        f = dict(rule=rule, resource_id=rid, resource_type="x", severity="HIGH", title="t", description="d",
                 recommendation="r", estimated_monthly_waste_usd=cat_waste)
        halluc = dict(f, resource_id="no-existe")
        return self._schema(findings=[f, halluc, f])  # duplicado y alucinado deben filtrarse


@pytest.fixture(autouse=True)
def fake_llm(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DISCOVERY_MODE", "mock")
    monkeypatch.setattr(base, "get_llm_from_config", lambda cfg: FakeLLM())
    monkeypatch.setattr(report, "get_llm_from_config", lambda cfg: FakeLLM())
    graph_mod.get_compiled_graph.cache_clear()


CFG = {"configurable": {"llm_provider": "openai", "llm_api_key": "sk-test"}}


@pytest.mark.parametrize("provider", ["AWS", "GCP", "Azure"])
def test_full_graph(provider: str) -> None:
    out = graph_mod.get_compiled_graph().invoke(
        {"provider": provider, "scope": "project", "target_id": "demo-123", "findings": [], "errors": []}, CFG)
    assert out["raw_infra_data"]["iam_roles"]
    assert len(out["findings"]) == 3                       # 1 por agente, filtrados
    assert {f["category"] for f in out["findings"]} == {"security", "governance", "cost"}
    assert out["kpis"]["cost"]["monthly_waste_usd"] == 50.0
    assert out["final_report"].startswith("# Informe")
    assert not out["errors"]


def test_discovery_deterministic() -> None:
    a = MockDiscoveryProvider("GCP").discover("project", "p1")
    b = MockDiscoveryProvider("GCP").discover("project", "p1")
    a["metadata"].pop("collected_at"); b["metadata"].pop("collected_at")
    for s in ("snapshots",):
        for x in a[s] + b[s]:
            x.pop("created_at")
    assert a == b


def test_kpis_empty() -> None:
    k = compute_kpis({}, [])
    assert k["overall_debt_score"] == 0 and k["grade"] == "A"


def test_missing_api_key() -> None:
    from backend.core.llm_config import LLMConfigError, LLMFactory, LLMSettings
    with pytest.raises(LLMConfigError):
        LLMFactory.create(LLMSettings(provider="openai", api_key=""))


def test_api_stream() -> None:
    import json

    from fastapi.testclient import TestClient

    from backend.main import app

    client = TestClient(app)
    body = {"provider": "AWS", "scope": "project", "target_id": "123456789012"}
    assert client.post("/api/v1/audit/stream", json=body).status_code == 401
    r = client.post("/api/v1/audit/stream", json=body, headers={"X-LLM-API-Key": "sk-test"})
    events = [json.loads(l) for l in r.text.splitlines()]
    assert events[0]["event"] == "start" and events[-1]["event"] == "completed"
    from backend.agent_graph.graph import NODE_SEQUENCE
    assert len([e for e in events if e["event"] == "node_completed"]) == len(NODE_SEQUENCE)
    assert events[-1]["result"]["discovery_status"] == "mock"
    assert "llm_api_key" not in r.text and "sk-test" not in r.text


def test_api_jobs() -> None:
    import time as _t

    from fastapi.testclient import TestClient

    from backend.main import app

    with TestClient(app) as client:
        body = {"provider": "AWS", "scope": "project", "target_id": "123456789012"}
        r = client.post("/api/v1/audits", json=body, headers={"X-LLM-API-Key": "sk-test"})
        assert r.status_code == 202
        jid = r.json()["job_id"]
        for _ in range(50):
            j = client.get(f"/api/v1/audits/{jid}").json()
            if j["status"] != "running":
                break
            _t.sleep(0.1)
        from backend.agent_graph.graph import NODE_SEQUENCE
        assert j["status"] == "completed" and len(j["events"]) == len(NODE_SEQUENCE)
        assert j["result"]["final_report"] and "sk-test" not in str(j)
        assert client.get("/api/v1/audits/nope").status_code == 404
