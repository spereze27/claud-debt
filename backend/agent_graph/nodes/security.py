"""Agente Security_Analyzer: mínimo privilegio (IAM) y exposición pública."""
from __future__ import annotations

from typing import Any

from backend.agent_graph.nodes.base import BaseAnalyzer
from backend.agent_graph.prompts import SECURITY_PROMPT


class SecurityAnalyzer(BaseAnalyzer):
    category = "security"
    system_prompt = SECURITY_PROMPT

    def select_data(self, raw: dict[str, Any]) -> dict[str, Any]:
        return {
            "iam_roles": raw.get("iam_roles", []),
            "storage_buckets": [{k: b[k] for k in ("resource_id", "public_access", "data_classification")}
                                for b in raw.get("storage_buckets", [])],
            "compute_instances": [{k: i[k] for k in ("resource_id", "public_ip", "firewall_ingress")}
                                  for i in raw.get("compute_instances", [])],
            "workloads": [{k: w.get(k) for k in ("resource_id", "service_type", "ingress", "public_invokers",
                                                 "runtime_service_account", "sa_explicitly_set", "sa_is_default",
                                                 "sa_project_roles", "plaintext_env_suspicious", "secrets_mounted")}
                          for w in raw.get("workloads", [])],
            "service_identities": [{k: i.get(k) for k in ("resource_id", "is_default_sa", "google_managed", "used_by",
                                                          "project_roles", "broad_roles", "privilege_escalation_roles",
                                                          "admin_roles", "user_managed_keys", "oldest_key_age_days")}
                                   for i in raw.get("service_identities", []) if not i.get("google_managed")],
            "databases": [{k: d.get(k) for k in ("resource_id", "public_ip", "authorized_networks", "open_to_internet",
                                                 "ssl_mode")} for d in raw.get("databases", [])],
            "public_access_edges": [e for e in (raw.get("architecture") or {}).get("edges", [])
                                    if e["kind"] == "public_access"],
        }


security_node = SecurityAnalyzer()
