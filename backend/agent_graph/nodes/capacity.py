"""Agente Capacity_Analyzer: dimensionamiento de clusters (Dataproc, GKE, Cloud SQL, Composer) con costo asociado."""
from __future__ import annotations

from typing import Any

from backend.agent_graph.nodes.base import BaseAnalyzer
from backend.agent_graph.prompts import CAPACITY_PROMPT
from backend.core.data_findings import capacity_findings


class CapacityAnalyzer(BaseAnalyzer):
    category = "capacity"
    system_prompt = CAPACITY_PROMPT

    def select_data(self, raw: dict[str, Any]) -> dict[str, Any]:
        dp = raw.get("data_platform") or {}
        flows_by_cluster: dict[str, list[str]] = {}
        for f in dp.get("flows") or []:
            if f.get("cluster"):
                flows_by_cluster.setdefault(f["cluster"], []).append(
                    f"{f['name']} (p95 {f.get('p95_duration_s')}s, {f.get('runs_in_window')} ejecuciones)")
        return {"clusters": [{k: v for k, v in c.items() if k not in ("labels",)}
                             | {"flows_running_here": flows_by_cluster.get(c.get("name"), [])[:15]}
                             for c in dp.get("clusters") or []]}

    def deterministic_findings(self, raw: dict[str, Any]) -> list[dict[str, Any]]:
        return capacity_findings(raw.get("data_platform") or {})


capacity_node = CapacityAnalyzer()
