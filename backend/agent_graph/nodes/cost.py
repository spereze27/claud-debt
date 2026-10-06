"""Agente Cost_Analyzer: recursos huérfanos e infrautilizados."""
from __future__ import annotations

from typing import Any

from backend.agent_graph.nodes.base import BaseAnalyzer
from backend.agent_graph.prompts import COST_PROMPT


class CostAnalyzer(BaseAnalyzer):
    category = "cost"
    system_prompt = COST_PROMPT

    def select_data(self, raw: dict[str, Any]) -> dict[str, Any]:
        return {
            "compute_instances": [{k: i[k] for k in ("resource_id", "machine_type", "avg_cpu_30d_pct", "monthly_cost_usd")}
                                  for i in raw.get("compute_instances", [])],
            "block_volumes": raw.get("block_volumes", []),
            "static_ips": raw.get("static_ips", []),
            "snapshots": raw.get("snapshots", []),
            "workloads": [{k: w.get(k) for k in ("resource_id", "service_type", "min_instances", "location")}
                          for w in raw.get("workloads", [])],
            "databases": [{k: d.get(k) for k in ("resource_id", "tier", "high_availability")} for d in raw.get("databases", [])],
            "billing": [raw["billing"]] if raw.get("billing") else [],
        }


cost_node = CostAnalyzer()
