"""Nodo Report_Generator: redacta el informe ejecutivo en Markdown."""
from __future__ import annotations

import json
import logging
from typing import Any

from langchain_core.messages import HumanMessage, SystemMessage
from langchain_core.runnables import RunnableConfig

from backend.agent_graph.prompts import REPORT_PROMPT
from backend.agent_graph.state import AuditState
from backend.core.llm_config import get_llm_from_config

logger = logging.getLogger(__name__)
_SEV_ORDER = {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2, "LOW": 3}


def _fallback_report(state: AuditState, error: str) -> str:
    k = state.get("kpis", {})
    rows = "\n".join(f"| {f['severity']} | `{f['resource_id']}` | {f['title']} |"
                     for f in sorted(state.get("findings", []), key=lambda f: _SEV_ORDER[f["severity"]]))
    errs = "\n".join(f"- {e}" for e in state.get("errors", []))
    return (f"# Informe de Deuda Técnica — {state['provider']} / {state['target_id']}\n\n"
            f"> ⚠️ El LLM no pudo redactar el informe ({error}). Se muestra la versión determinista.\n\n"
            f"**Deuda global:** {k.get('overall_debt_score', 'N/A')} (grado {k.get('grade', 'N/A')})\n\n"
            + (f"## ⚠️ Problemas de cobertura\n{errs}\n\n" if errs else "")
            + f"| Severidad | Recurso | Hallazgo |\n|---|---|---|\n{rows}\n")


def report_node(state: AuditState, config: RunnableConfig) -> dict[str, Any]:
    findings = sorted(state.get("findings", []), key=lambda f: _SEV_ORDER[f["severity"]])
    raw = state.get("raw_infra_data") or {}
    arch = raw.get("architecture") or {}
    labels = {n["id"]: f'{n["type"]}: {n["label"]}' for n in arch.get("nodes", [])}
    flows = [{"from": labels.get(e["source"], e["source"]), "mechanism": e["kind"],
              "to": labels.get(e["target"], e["target"]), "identity": e.get("via"),
              "permissions": e.get("permissions"), "detail": e.get("detail"), "inferred": e.get("inferred")}
             for e in arch.get("edges", []) if e["kind"] != "has_role"][:150]
    payload = {
        "context": {"provider": state["provider"], "scope": state["scope"], "target_id": state["target_id"],
                    "discovery_status": state.get("discovery_status", "unknown"),
                    "analysis_errors": state.get("errors", [])},
        "kpis": state.get("kpis", {}),
        "architecture": {"flows": flows, "asset_counts": raw.get("asset_counts"),
                         "enabled_apis": raw.get("enabled_apis", [])[:60]},
        "billing": raw.get("billing"),
        "data_cascades": [{k: g.get(k) for k in ("grupo_cascada_id", "origen_inferencia", "num_flujos", "flujos_en_orden",
                                                  "capas_recorridas", "owners", "costo_30d_usd")}
                          for g in (raw.get("data_platform") or {}).get("cascades") or []][:30],
        "findings": [{k: f[k] for k in ("severity", "category", "rule", "resource_id", "title", "recommendation",
                                         "estimated_monthly_waste_usd")} for f in findings],
    }
    try:
        llm = get_llm_from_config(config)
        msg = llm.invoke([
            SystemMessage(content=REPORT_PROMPT.format(provider=state["provider"], target_id=state["target_id"])),
            HumanMessage(content="Datos de la auditoría:\n```json\n" + json.dumps(payload, indent=1, ensure_ascii=False) + "\n```"),
        ])
        content = msg.content if isinstance(msg.content, str) else "".join(
            p.get("text", "") if isinstance(p, dict) else str(p) for p in msg.content)
        return {"final_report": content}
    except Exception as exc:  # noqa: BLE001
        logger.exception("Report generator falló")
        return {"final_report": _fallback_report(state, type(exc).__name__),
                "errors": [f"report: {type(exc).__name__}: {exc}"]}
