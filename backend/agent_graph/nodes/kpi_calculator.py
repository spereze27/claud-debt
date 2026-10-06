"""Nodo KPI_Calculator: 100% determinista (sin LLM)."""
from __future__ import annotations

from typing import Any

from backend.agent_graph.state import AuditState
from backend.core.kpi_engine import compute_kpis


def kpi_calculator_node(state: AuditState) -> dict[str, Any]:
    try:
        return {"kpis": compute_kpis(state.get("raw_infra_data") or {}, list(state.get("findings") or []))}
    except Exception as exc:  # noqa: BLE001
        return {"kpis": {}, "errors": [f"kpi_calculator: {exc}"]}
