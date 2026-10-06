"""Agente Pipeline_Analyzer: rendimiento, contradicciones, inactividad y entradas/salidas de cada flujo."""
from __future__ import annotations

from typing import Any

from backend.agent_graph.nodes.base import BaseAnalyzer
from backend.agent_graph.prompts import PIPELINES_PROMPT
from backend.core.data_findings import pipeline_findings

FLOW_FIELDS = ("resource_id", "project_id", "name", "kind", "engine", "schedule", "identity", "state", "declared_state",
               "runs_in_window", "failures", "failure_rate_pct", "last_run", "last_status", "days_since_last_run",
               "avg_duration_s", "p95_duration_s", "duration_trend_pct", "bytes_processed_30d", "slot_hours_30d",
               "cost_30d_usd", "cost_basis", "streaming", "requests_30d", "signals", "description", "owner",
               "grupo_cascada_id", "origen_grupo", "orden_en_cascada", "capa_entrada_inferida", "capa_salida_inferida",
               "fuente_raw", "destino_raw")


class PipelineAnalyzer(BaseAnalyzer):
    category = "pipelines"
    system_prompt = PIPELINES_PROMPT

    def select_data(self, raw: dict[str, Any]) -> dict[str, Any]:
        dp = raw.get("data_platform") or {}
        flows = sorted(dp.get("flows") or [], key=lambda f: (-len(f.get("signals") or []), -f["cost_30d_usd"]))[:120]
        return {
            "flows": [{**{k: f.get(k) for k in FLOW_FIELDS},
                       "inputs": [{k: i.get(k) for k in ("name", "system", "layer", "format")} for i in f["inputs"]][:12],
                       "outputs": [{k: o.get(k) for k in ("name", "system", "layer", "write_mode")} for o in f["outputs"]][:12],
                       "sample_query": (f.get("sample_query") or "")[:500]} for f in flows],
            "detected_issues": [i for i in dp.get("issues") or []][:150],
            "cascades": [{k: v for k, v in g.items() if k != "flujo_ids"} | {"resource_id": g["flujo_ids"][0]}
                         for g in dp.get("cascades") or []][:60],
            "history_days": [{"resource_id": "_", "value": dp.get("history_days")}],
        }

    def is_empty(self, data: dict[str, Any]) -> bool:
        return not data["flows"]

    def deterministic_findings(self, raw: dict[str, Any]) -> list[dict[str, Any]]:
        return pipeline_findings(raw.get("data_platform") or {})


pipelines_node = PipelineAnalyzer()
