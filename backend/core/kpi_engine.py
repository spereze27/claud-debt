"""Motor determinista de KPIs de Deuda Técnica (sin LLM, 100% reproducible).

Los KPIs se calculan cruzando los *findings* (numerador: recursos afectados,
deduplicados por resource_id) con el inventario descubierto (denominador).
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable

from backend.core.data_findings import all_rules as _data_rules

SEVERITY_WEIGHTS: dict[str, int] = {"CRITICAL": 10, "HIGH": 6, "MEDIUM": 3, "LOW": 1}

# Catálogo cerrado de reglas: los prompts obligan a los LLM a usar estos códigos.
RULES: dict[str, str] = {
    # Seguridad
    "IAM_ADMIN_ROLE": "security",
    "IAM_WILDCARD_PERMISSION": "security",
    "IAM_STALE_KEY": "security",
    "PUBLIC_BUCKET": "security",
    "PUBLIC_INSTANCE": "security",
    "OPEN_FIREWALL": "security",
    "PUBLIC_SERVICE": "security",
    "DEFAULT_SA_IN_USE": "security",
    "SA_OVERPRIVILEGED": "security",
    "SECRET_IN_ENV": "security",
    "PUBLIC_DATABASE": "security",
    # Gobierno
    "MISSING_TAGS": "governance",
    "UNENCRYPTED_BUCKET": "governance",
    "UNENCRYPTED_DISK": "governance",
    "NO_CMK_ENCRYPTION": "governance",
    "NO_DB_BACKUP": "governance",
    # Costos
    "ORPHAN_DISK": "cost",
    "ORPHAN_IP": "cost",
    "IDLE_INSTANCE": "cost",
    "OLD_SNAPSHOT": "cost",
    "NO_BUDGET": "cost",
    "ALWAYS_ON_SERVICE": "cost",
}
RULES.update(_data_rules())

CATEGORY_WEIGHTS = {"security": 0.30, "governance": 0.15, "cost": 0.15,
                    "data_governance": 0.15, "pipelines": 0.15, "capacity": 0.10}


@dataclass(frozen=True)
class Inventory:
    iam_roles: int
    buckets: int
    instances: int
    disks: int
    ips: int
    snapshots: int
    workloads: int = 0
    identities: int = 0
    databases: int = 0

    @property
    def taggable(self) -> int:
        return self.buckets + self.instances + self.disks + self.workloads + self.databases

    @classmethod
    def from_raw(cls, raw: dict[str, Any]) -> "Inventory":
        return cls(
            iam_roles=len(raw.get("iam_roles", [])),
            buckets=len(raw.get("storage_buckets", [])),
            instances=len(raw.get("compute_instances", [])),
            disks=len(raw.get("block_volumes", [])),
            ips=len(raw.get("static_ips", [])),
            snapshots=len(raw.get("snapshots", [])),
            workloads=len(raw.get("workloads", [])),
            identities=len([i for i in raw.get("service_identities", []) if not i.get("google_managed")]),
            databases=len(raw.get("databases", [])),
        )


def _pct(part: int, total: int) -> float:
    return round(100.0 * part / total, 1) if total else 0.0


def _affected(findings: Iterable[dict[str, Any]], *rules: str) -> int:
    return len({f["resource_id"] for f in findings if f.get("rule") in rules})


def _category_score(findings: list[dict[str, Any]], category: str, n_resources: int) -> float:  # noqa: D401
    """Índice 0-100 de deuda (100 = deuda máxima), normalizado por tamaño del inventario."""
    points = sum(SEVERITY_WEIGHTS.get(f["severity"], 1) for f in findings if f["category"] == category)
    max_points = max(n_resources, 1) * SEVERITY_WEIGHTS["HIGH"]
    return round(min(100.0, 100.0 * points / max_points), 1)


def _grade(score: float) -> str:
    for threshold, grade in ((15, "A"), (30, "B"), (50, "C"), (70, "D")):
        if score < threshold:
            return grade
    return "E"


def compute_kpis(raw_infra: dict[str, Any], findings: list[dict[str, Any]]) -> dict[str, Any]:
    inv = Inventory.from_raw(raw_infra)
    valid = [f for f in findings if f.get("rule") in RULES]

    security = {
        "pct_admin_roles": _pct(_affected(valid, "IAM_ADMIN_ROLE"), inv.iam_roles),
        "pct_wildcard_roles": _pct(_affected(valid, "IAM_WILDCARD_PERMISSION"), inv.iam_roles),
        "pct_public_buckets": _pct(_affected(valid, "PUBLIC_BUCKET"), inv.buckets),
        "pct_public_instances": _pct(_affected(valid, "PUBLIC_INSTANCE", "OPEN_FIREWALL"), inv.instances),
        "pct_public_workloads": _pct(_affected(valid, "PUBLIC_SERVICE"), inv.workloads),
        "pct_workloads_default_sa": _pct(_affected(valid, "DEFAULT_SA_IN_USE"), inv.workloads),
        "pct_overprivileged_identities": _pct(_affected(valid, "SA_OVERPRIVILEGED"), inv.identities),
        "secrets_in_env": _affected(valid, "SECRET_IN_ENV"),
        "public_databases": _affected(valid, "PUBLIC_DATABASE"),
    }
    governance = {
        "pct_untagged_resources": _pct(_affected(valid, "MISSING_TAGS"), inv.taggable),
        "pct_unencrypted_buckets": _pct(_affected(valid, "UNENCRYPTED_BUCKET"), inv.buckets),
        "pct_unencrypted_disks": _pct(_affected(valid, "UNENCRYPTED_DISK"), inv.disks),
        "pct_db_without_backup": _pct(_affected(valid, "NO_DB_BACKUP"), inv.databases),
    }
    orphan_total = inv.disks + inv.ips + inv.snapshots
    cost = {
        "orphan_resources": _affected(valid, "ORPHAN_DISK", "ORPHAN_IP", "OLD_SNAPSHOT"),
        "pct_orphan_resources": _pct(_affected(valid, "ORPHAN_DISK", "ORPHAN_IP", "OLD_SNAPSHOT"), orphan_total),
        "pct_idle_instances": _pct(_affected(valid, "IDLE_INSTANCE"), inv.instances),
        # Suma el ahorro/desperdicio estimado de TODAS las categorías (costos, capacidad, flujos, almacenamiento).
        "monthly_waste_usd": round(sum(float(f.get("estimated_monthly_waste_usd", 0) or 0) for f in valid), 2),
        "always_on_services": _affected(valid, "ALWAYS_ON_SERVICE"),
        "has_budget": (raw_infra.get("billing") or {}).get("has_budget"),
        "billing_enabled": (raw_infra.get("billing") or {}).get("billing_enabled"),
        "total_cost_30d_usd": (raw_infra.get("billing") or {}).get("total_cost_30d_usd"),
    }

    scores = {
        "security": _category_score(valid, "security", inv.iam_roles + inv.buckets + inv.instances + inv.workloads + inv.databases),
        "governance": _category_score(valid, "governance", inv.taggable),
        "cost": _category_score(valid, "cost", orphan_total + inv.instances + inv.workloads + 1),
    }
    dp = raw_infra.get("data_platform") or {}
    tables, flows, lake, clusters = (dp.get("tables") or [], dp.get("flows") or [], dp.get("lake") or [],
                                     dp.get("clusters") or [])
    summary = dp.get("summary") or {}
    scores["data_governance"] = _category_score(valid, "data_governance", len(tables) + len(lake) + len(flows))
    scores["pipelines"] = _category_score(valid, "pipelines", len(flows))
    scores["capacity"] = _category_score(valid, "capacity", len(clusters))
    # Sólo ponderan las categorías que tienen algo que evaluar.
    has = {"security": True, "governance": True, "cost": True, "data_governance": bool(tables or lake or flows),
           "pipelines": bool(flows), "capacity": bool(clusters)}
    wsum = sum(w for c, w in CATEGORY_WEIGHTS.items() if has[c])
    overall = round(sum(scores[c] * w for c, w in CATEGORY_WEIGHTS.items() if has[c]) / wsum, 1)

    n_flows = len(flows)
    data_gov = {
        "tables": len(tables), "buckets": len(lake),
        "tables_by_layer": summary.get("tables_by_layer", {}),
        "pct_tables_with_layer": summary.get("pct_tables_with_layer"),
        "pct_large_tables_partitioned": summary.get("pct_large_tables_partitioned"),
        "layer_violations": _affected(valid, "LAYER_INVERSION", "LAYER_SKIP", "LAYER_MISMATCH", "BRONZE_OVERWRITE"),
        "normalization_issues": _affected(valid, "NORMALIZATION_ISSUE"),
        "schema_type_issues": _affected(valid, "SCHEMA_TYPE_ISSUE"),
        "tables_with_pii": summary.get("tables_with_pii", 0),
        "lake_issues": _affected(valid, "WRONG_FILE_FORMAT", "SMALL_FILES", "WRONG_STORAGE_CLASS", "NO_PARTITION_LAYOUT"),
    }
    by_state = summary.get("flows_by_state") or {}
    pipelines = {
        "flows": n_flows, "flows_by_kind": summary.get("flows_by_kind", {}),
        "pct_inactive": _pct(by_state.get("inactive", 0), n_flows),
        "pct_failing": _pct(by_state.get("failing", 0), n_flows),
        "conflicts": _affected(valid, "FLOW_CONFLICT"), "cycles": _affected(valid, "FLOW_CYCLE"),
        "slow_flows": _affected(valid, "SLOW_FLOW"), "full_scans": _affected(valid, "FULL_SCAN", "FULL_RECOMPUTE"),
        "flows_cost_30d_usd": summary.get("flows_cost_30d_usd", 0.0),
    }
    sized = [c.get("sizing") or {} for c in clusters]
    capacity = {
        "clusters": len(clusters),
        "oversized": sum(1 for s in sized if s.get("verdict") == "oversized"),
        "undersized": sum(1 for s in sized if s.get("verdict") == "undersized"),
        "no_data": sum(1 for s in sized if s.get("verdict") == "no_data"),
        "current_monthly_usd": round(sum(s.get("current_monthly_usd") or 0 for s in sized), 2),
        "potential_saving_usd": round(sum(s.get("total_potential_saving_usd") or 0 for s in sized), 2),
    }
    cost["data_platform_total_usd"] = (dp.get("cost_breakdown") or {}).get("total_usd")

    by_severity = {s: sum(1 for f in valid if f["severity"] == s) for s in SEVERITY_WEIGHTS}

    return {
        "inventory": inv.__dict__,
        "security": security,
        "governance": governance,
        "cost": cost,
        "data_governance": data_gov,
        "pipelines": pipelines,
        "capacity": capacity,
        "debt_scores": scores,
        "overall_debt_score": overall,
        "grade": _grade(overall),
        "findings_by_severity": by_severity,
        "total_findings": len(valid),
        "discarded_findings": len(findings) - len(valid),
    }
