"""Nodo Data_Report_Generator: informe detallado de datos, flujos, capacidad y costos por flujo.

= narrativa del LLM (análisis y recomendaciones) + anexo técnico determinista (cifras exactas).
"""
from __future__ import annotations

import json
import logging
from typing import Any

from langchain_core.messages import HumanMessage, SystemMessage
from langchain_core.runnables import RunnableConfig

from backend.agent_graph.prompts import DATA_REPORT_PROMPT
from backend.agent_graph.state import AuditState
from backend.core.data_report import build_appendix
from backend.core.llm_config import get_llm_from_config

logger = logging.getLogger(__name__)
DATA_CATEGORIES = {"data_governance", "pipelines", "capacity"}
MAX_CHARS = 170_000


def _payload(state: AuditState) -> dict[str, Any]:
    raw = state.get("raw_infra_data") or {}
    dp = raw.get("data_platform") or {}
    flows = sorted(dp.get("flows") or [], key=lambda f: (-len(f.get("signals") or []), -f["cost_30d_usd"]))[:70]
    tables = dp.get("tables") or []
    by_layer: dict[str, int] = {}
    for t in tables:
        by_layer[t["layer"] or "sin capa"] = by_layer.get(t["layer"] or "sin capa", 0) + 1
    return {
        "context": {"provider": state["provider"], "scope": state["scope"], "target_id": state["target_id"],
                    "projects": raw.get("projects"), "history_days": dp.get("history_days"),
                    "discovery_status": state.get("discovery_status"),
                    "collection_warnings": [e for e in state.get("errors", []) if "discovery" in e][:15]},
        "kpis": {k: (state.get("kpis") or {}).get(k) for k in ("data_governance", "pipelines", "capacity", "debt_scores",
                                                                   "overall_debt_score", "grade")},
        "flows": [{k: f.get(k) for k in ("resource_id", "project_id", "folder_path", "name", "kind", "engine", "schedule",
                                         "state_es", "runs_in_window", "failure_rate_pct", "last_run", "avg_duration_s",
                                         "p95_duration_s", "duration_trend_pct", "cost_30d_usd", "cost_basis",
                                         "identity", "description", "grupo_cascada_id", "origen_grupo",
                                         "orden_en_cascada", "capa_entrada_inferida", "capa_salida_inferida")}
                  | {"inputs": [{k: i.get(k) for k in ("name", "system", "layer", "format", "structure")} for i in f["inputs"]][:8],
                     "outputs": [{k: o.get(k) for k in ("name", "system", "layer", "write_mode")} for o in f["outputs"]][:8],
                     "signals": [s["text"] for s in f.get("signals") or []][:6],
                     "sample_query": (f.get("sample_query") or "")[:300]} for f in flows],
        "tables_by_layer": by_layer,
        "tables_flagged": [{k: t.get(k) for k in ("dataset", "table", "layer", "size_gb", "normalization",
                                                  "expected_for_layer", "partitioned")}
                           | {"signals": [s["text"] for s in t["signals"] if s["severity"] != "LOW"][:4]}
                           for t in tables if any(s["severity"] != "LOW" for s in t["signals"])][:50],
        "datasets": [{k: d.get(k) for k in ("dataset", "layer", "tables", "size_gb", "storage_billing_model",
                                            "recommended_billing_model", "billing_model_saving_usd", "layers_mixed")}
                     for d in dp.get("datasets") or []][:60],
        "lake": [{k: b.get(k) for k in ("bucket", "layer", "layers_in_prefixes", "storage_class", "size_gb",
                                        "formats_by_bytes", "small_files_pct", "partition_style", "median_age_days")}
                 | {"signals": [s["text"] for s in b["signals"]]} for b in dp.get("lake") or []],
        "clusters": [{k: c.get(k) for k in ("name", "kind", "project_id", "utilization")} | {"sizing": c.get("sizing")}
                     for c in dp.get("clusters") or []],
        "issues": dp.get("issues", [])[:80],
        "cascades": [{k: v for k, v in g.items() if k != "flujo_ids"} for g in dp.get("cascades") or []][:60],
        "cost_breakdown": dp.get("cost_breakdown_by_project") or [dp.get("cost_breakdown")],
        "real_billing": (raw.get("billing") or {}).get("cost_by_service_30d"),
        "agent_findings": [{k: f[k] for k in ("category", "rule", "resource_id", "severity", "title", "description",
                                               "recommendation", "estimated_monthly_waste_usd")}
                           for f in state.get("findings", []) if f["category"] in DATA_CATEGORIES][:90],
    }


def data_report_node(state: AuditState, config: RunnableConfig) -> dict[str, Any]:
    raw = state.get("raw_infra_data") or {}
    appendix = build_appendix(raw)
    if not appendix:
        return {"data_report": "_No se encontraron flujos, tablas ni clusters para el informe de datos. "
                               "Revisa los permisos de BigQuery/Dataflow/Dataproc en las advertencias._"}
    body = json.dumps(_payload(state), ensure_ascii=False, default=str, separators=(",", ":"))
    if len(body) > MAX_CHARS:
        body = body[:MAX_CHARS] + "…(truncado)"
    try:
        llm = get_llm_from_config(config)
        msg = llm.invoke([
            SystemMessage(content=DATA_REPORT_PROMPT.format(provider=state["provider"], target_id=state["target_id"])),
            HumanMessage(content="Datos de la plataforma (JSON):\n```json\n" + body + "\n```"),
        ])
        text = msg.content if isinstance(msg.content, str) else "".join(
            p.get("text", "") if isinstance(p, dict) else str(p) for p in msg.content)
        return {"data_report": text.strip() + "\n" + appendix}
    except Exception as exc:  # noqa: BLE001
        logger.exception("Data report falló")
        return {"data_report": f"> ⚠️ El LLM no pudo redactar el análisis ({type(exc).__name__}). "
                               f"Se incluye el anexo técnico determinista.\n{appendix}",
                "errors": [f"data_report: {type(exc).__name__}: {exc}"]}
