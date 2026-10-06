"""Agente Data_Governance_Analyzer: capas medallion, normalización, esquemas, data lake y almacenamiento."""
from __future__ import annotations

from typing import Any

from backend.agent_graph.nodes.base import BaseAnalyzer
from backend.agent_graph.prompts import DATA_GOVERNANCE_PROMPT
from backend.core.data_findings import data_governance_findings

TABLE_FIELDS = ("resource_id", "dataset", "table", "type", "layer", "layer_source", "size_gb", "rows", "partitioned",
                "partition_column", "clustering", "columns_count", "nested_fields", "repeated_fields",
                "fk_like_columns", "id_description_pairs", "repeating_groups", "normalization", "expected_for_layer",
                "string_date_columns", "numeric_as_string_columns", "pii_columns", "description_present",
                "days_since_modified", "writers", "readers", "consumer_queries_30d", "signals")


class DataGovernanceAnalyzer(BaseAnalyzer):
    category = "data_governance"
    system_prompt = DATA_GOVERNANCE_PROMPT

    def select_data(self, raw: dict[str, Any]) -> dict[str, Any]:
        dp = raw.get("data_platform") or {}
        tables = sorted(dp.get("tables") or [], key=lambda t: (-len(t.get("signals") or []), -t["size_gb"]))[:150]
        flows = [{"resource_id": f["resource_id"], "name": f["name"], "kind": f["kind"],
                  "inputs": [(i["name"], i.get("layer"), i.get("format")) for i in f["inputs"]][:10],
                  "outputs": [(o["name"], o.get("layer"), o.get("write_mode")) for o in f["outputs"]][:10],
                  "sample_query": (f.get("sample_query") or "")[:400]}
                 for f in dp.get("flows") or [] if f["outputs"]][:120]
        return {
            "tables": [{k: t.get(k) for k in TABLE_FIELDS} for t in tables],
            "datasets": dp.get("datasets") or [],
            "lake_buckets": [{k: v for k, v in b.items() if k != "top_prefixes"} | {"prefixes": b.get("top_prefixes", [])[:8]}
                             for b in dp.get("lake") or []],
            "flows": flows,
            "clusters_with_hdfs": [{"resource_id": c["resource_id"], "name": c["name"]}
                                   for c in dp.get("clusters") or [] if c.get("hdfs_heavy")],
        }

    def is_empty(self, data: dict[str, Any]) -> bool:
        return not (data["tables"] or data["lake_buckets"] or data["flows"])

    def deterministic_findings(self, raw: dict[str, Any]) -> list[dict[str, Any]]:
        return data_governance_findings(raw.get("data_platform") or {})


data_governance_node = DataGovernanceAnalyzer()
