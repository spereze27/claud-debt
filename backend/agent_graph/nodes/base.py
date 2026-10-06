"""Clase base para los agentes analizadores (Template Method + structured output).

Flujo de cada analizador:
  1. `select_data`  → porción del inventario relevante para el agente (menos tokens, menos ruido).
  2. LLM con salida estructurada → hallazgos.
  3. Guardrail anti-alucinación: reglas del catálogo de la categoría + resource_id existente.
  4. `deterministic_findings` → hallazgos por reglas que el LLM haya omitido (garantía de cobertura).
"""
from __future__ import annotations

import json
import logging
import uuid
from abc import ABC, abstractmethod
from typing import Any, Literal

from langchain_core.messages import HumanMessage, SystemMessage
from langchain_core.runnables import RunnableConfig
from pydantic import BaseModel, Field

from backend.agent_graph.state import AuditState, Finding
from backend.core.kpi_engine import RULES
from backend.core.llm_config import get_llm_from_config

logger = logging.getLogger(__name__)
MAX_PROMPT_CHARS = 180_000   # ~45k tokens: tope para no exceder la ventana de contexto de modelos pequeños


class FindingModel(BaseModel):
    rule: str = Field(description="Código de regla del catálogo permitido")
    resource_id: str = Field(description="resource_id exacto del inventario")
    resource_type: str
    severity: Literal["CRITICAL", "HIGH", "MEDIUM", "LOW"]
    title: str
    description: str
    recommendation: str
    estimated_monthly_waste_usd: float = 0.0


class FindingsResponse(BaseModel):
    findings: list[FindingModel] = Field(default_factory=list)


class BaseAnalyzer(ABC):
    category: str
    system_prompt: str

    @abstractmethod
    def select_data(self, raw: dict[str, Any]) -> dict[str, Any]:
        """Sólo se envía al LLM la porción del inventario relevante."""

    def deterministic_findings(self, raw: dict[str, Any]) -> list[dict[str, Any]]:
        """Hallazgos por reglas (opcional). Se añaden si el LLM no reportó el mismo (regla, recurso)."""
        return []

    def is_empty(self, data: dict[str, Any]) -> bool:
        return not any(v for v in data.values())

    def _known_ids(self, data: dict[str, Any]) -> set[str]:
        return {r["resource_id"] for v in data.values() if isinstance(v, list) for r in v
                if isinstance(r, dict) and "resource_id" in r}

    def _to_finding(self, f: dict[str, Any]) -> Finding:
        f = {k: f[k] for k in ("rule", "resource_id", "resource_type", "severity", "title", "description",
                               "recommendation", "estimated_monthly_waste_usd")}
        return Finding(id=f"{self.category[:3].upper()}-{uuid.uuid4().hex[:8]}", category=self.category, **f)  # type: ignore[typeddict-item]

    def _validate(self, resp: FindingsResponse, data: dict[str, Any], extra_ids: set[str] | None = None) -> list[Finding]:
        """Guardrail anti-alucinación: descarta reglas fuera de catálogo o recursos inexistentes."""
        known, seen, out = self._known_ids(data) | (extra_ids or set()), set(), []
        for f in resp.findings:
            key = (f.rule, f.resource_id)
            if RULES.get(f.rule) != self.category or f.resource_id not in known or key in seen:
                logger.warning("%s: finding descartado %s", self.category, key)
                continue
            seen.add(key)
            out.append(self._to_finding(f.model_dump()))
        return out

    def _merge_deterministic(self, llm: list[Finding], raw: dict[str, Any]) -> list[Finding]:
        have = {(f["rule"], f["resource_id"]) for f in llm}
        extra = [self._to_finding(d) for d in self.deterministic_findings(raw)
                 if (d["rule"], d["resource_id"]) not in have and RULES.get(d["rule"]) == self.category]
        return llm + extra

    def __call__(self, state: AuditState, config: RunnableConfig) -> dict[str, Any]:
        raw = state.get("raw_infra_data") or {}
        if not raw:
            return {"errors": [f"{self.category}: sin inventario para analizar"]}
        if state.get("discovery_status") == "failed":
            return {"findings": []}  # el fallo ya está registrado por discovery; evita gastar tokens
        data = self.select_data(raw)
        if self.is_empty(data):
            return {"findings": []}
        try:
            llm = get_llm_from_config(config).with_structured_output(FindingsResponse)
            system = self.system_prompt.format(provider=state["provider"])
            body = json.dumps({"metadata": {k: v for k, v in (raw.get("metadata") or {}).items()
                                            if k != "collection_errors"}, **data},
                              indent=1, default=str, ensure_ascii=False)
            if len(body) > MAX_PROMPT_CHARS:
                body = json.dumps({"metadata": raw.get("metadata"), **data}, separators=(",", ":"), default=str,
                                  ensure_ascii=False)[:MAX_PROMPT_CHARS] + "\n…(truncado)"
            human = "Inventario a auditar (JSON):\n```json\n" + body + "\n```"
            resp = llm.invoke([SystemMessage(content=system), HumanMessage(content=human)])
            if not isinstance(resp, FindingsResponse):
                resp = FindingsResponse.model_validate(resp)
            findings = self._merge_deterministic(self._validate(resp, data), raw)
            logger.info("%s: %d findings", self.category, len(findings))
            return {"findings": findings}
        except Exception as exc:  # noqa: BLE001 — un agente caído no tumba la auditoría
            logger.exception("%s analyzer falló", self.category)
            fallback = self._merge_deterministic([], raw)
            return {"findings": fallback,
                    "errors": [f"{self.category}: {type(exc).__name__}: {exc}"
                               + (f" (se conservaron {len(fallback)} hallazgos deterministas)" if fallback else "")]}
