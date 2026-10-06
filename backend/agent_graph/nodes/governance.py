"""Agente Governance_Analyzer: etiquetado y encriptación."""
from __future__ import annotations

from typing import Any

from backend.agent_graph.nodes.base import BaseAnalyzer
from backend.agent_graph.prompts import GOVERNANCE_PROMPT


class GovernanceAnalyzer(BaseAnalyzer):
    category = "governance"
    system_prompt = GOVERNANCE_PROMPT

    def select_data(self, raw: dict[str, Any]) -> dict[str, Any]:
        return {
            "storage_buckets": [{k: b[k] for k in ("resource_id", "encryption", "data_classification", "tags")}
                                for b in raw.get("storage_buckets", [])],
            "compute_instances": [{k: i[k] for k in ("resource_id", "tags")}
                                  for i in raw.get("compute_instances", [])],
            "block_volumes": [{k: v[k] for k in ("resource_id", "encrypted", "tags")}
                              for v in raw.get("block_volumes", [])],
            "workloads": [{k: w.get(k) for k in ("resource_id", "service_type", "tags")} for w in raw.get("workloads", [])],
            "databases": [{k: d.get(k) for k in ("resource_id", "engine", "backups_enabled", "high_availability",
                                                 "deletion_protection", "tags")} for d in raw.get("databases", [])],
        }


governance_node = GovernanceAnalyzer()
