"""Dimensionamiento de clusters (Dataproc, GKE, Cloud SQL, Composer) — 100% determinista.

Regla base: se busca que el percentil 95 de uso quede cerca del 65 %.
  · p95 máx(CPU, memoria) < 35 %  → SOBREDIMENSIONADO
  · p95 CPU > 85 % o memoria > 90 % → SUBDIMENSIONADO
  · resto                           → ADECUADO
Además se evalúan señales de configuración (autoscaling, borrado por inactividad,
workers preemptibles, HDFS en disco persistente) con su ahorro estimado.
"""
from __future__ import annotations

from typing import Any

from backend.core import pricing as P

TARGET = 65.0
OVER, UNDER_CPU, UNDER_MEM = 35.0, 85.0, 90.0


def _fmt(x: float) -> str:
    return f"${x:,.2f}"


def _groups_cost(groups: list[dict[str, Any]], dataproc: bool, hours: float = P.HOURS_PER_MONTH) -> float:
    return round(sum(P.node_group_monthly(g.get("machine_type"), int(g.get("count") or 0), bool(g.get("preemptible")),
                                          dataproc, hours, float(g.get("disk_gb") or 0), g.get("disk_type") or "pd-standard")
                     for g in groups), 2)


def _describe(groups: list[dict[str, Any]]) -> str:
    return "; ".join(f"{g['role']}: {g.get('count')}× {g.get('machine_type')}"
                     + (" (preemptible)" if g.get("preemptible") else "") for g in groups if g.get("count"))


def _verdict(util: dict[str, Any] | None) -> tuple[str, float | None]:
    if not util or util.get("cpu_p95") is None:
        return "no_data", None
    cpu = float(util["cpu_p95"])
    mem = util.get("mem_p95")
    peak = max(cpu, float(mem)) if mem is not None else cpu
    if cpu > UNDER_CPU or (mem is not None and float(mem) > UNDER_MEM):
        return "undersized", peak
    if peak < OVER:
        return "oversized", peak
    return "right_sized", peak


def _util_text(util: dict[str, Any]) -> str:
    parts = [f"CPU p95 {util['cpu_p95']:.0f}% (media {util.get('cpu_avg', 0):.0f}%)"]
    if util.get("mem_p95") is not None:
        parts.append(f"memoria p95 {util['mem_p95']:.0f}% (media {util.get('mem_avg', 0):.0f}%)")
    return " y ".join(parts) + f" en los últimos {util.get('days', 14)} días"


def _resize_groups(groups: list[dict[str, Any]], ratio: float, min_workers: int) -> tuple[list[dict[str, Any]], list[str]]:
    """Escala los grupos escalables por nº de nodos; si no alcanza, reduce el tamaño de máquina."""
    out, notes = [], []
    for g in groups:
        g2 = dict(g)
        count = int(g.get("count") or 0)
        if g.get("scalable") and count > 0:
            new = max(min_workers, P.ceil_int(count * ratio))
            if ratio < 1 and new == count and count <= min_workers:
                shape = P.parse_machine_type(g.get("machine_type"))
                if shape and shape.vcpus >= 4:
                    smaller = P.machine_with_vcpus(shape, max(2, P.ceil_int(shape.vcpus * max(ratio, 0.5))))
                    if smaller and smaller != shape.name:
                        g2["machine_type"] = smaller
                        notes.append(f"{g['role']}: {shape.name} → {smaller} (ya está en el mínimo de nodos)")
            elif new != count:
                g2["count"] = new
                notes.append(f"{g['role']}: {count} → {new} nodos")
        out.append(g2)
    return out, notes


def size_cluster(c: dict[str, Any]) -> dict[str, Any]:
    kind = c.get("kind")
    util = c.get("utilization")
    verdict, peak = _verdict(util)
    reasons: list[str] = []
    extras: list[dict[str, Any]] = []
    rec_desc, rec_cost = None, None

    if kind in ("dataproc", "gke"):
        dataproc = kind == "dataproc"
        groups = c.get("node_groups") or []
        current = _groups_cost(groups, dataproc) + (P.GKE_CLUSTER_FEE_H * P.HOURS_PER_MONTH if kind == "gke" else 0)
        if verdict in ("oversized", "undersized"):
            ratio = peak / TARGET if peak else 1.0
            new_groups, notes = _resize_groups(groups, ratio, 2 if dataproc else 1)
            rec_cost = _groups_cost(new_groups, dataproc) + (P.GKE_CLUSTER_FEE_H * P.HOURS_PER_MONTH if kind == "gke" else 0)
            rec_desc = "; ".join(notes) or _describe(new_groups)
            reasons.append(f"{_util_text(util)}. Para un objetivo de {TARGET:.0f}% de uso p95 se necesita "
                           f"~{ratio * 100:.0f}% de la capacidad actual.")
        elif verdict == "right_sized":
            reasons.append(f"{_util_text(util)}: el tamaño es coherente con la carga.")
        else:
            reasons.append("Sin métricas de uso (Cloud Monitoring) para este cluster: sólo se evalúa la configuración.")

        if not c.get("autoscaling"):
            reasons.append("No tiene autoscaling: paga la capacidad pico todo el tiempo.")
            if util and util.get("cpu_avg") is not None and peak:
                avg = float(util["cpu_avg"])
                saving = current * max(0.0, 1 - max(avg, 20) / max(peak, 1)) * 0.5
                if saving > 5:
                    extras.append({"action": "Habilitar autoscaling (política basada en YARN/HPA)",
                                   "monthly_saving": round(saving, 2)})
        if dataproc:
            active = (util or {}).get("active_hours_pct")
            if not c.get("idle_delete_ttl_s") and c.get("state") == "RUNNING":
                reasons.append("Cluster persistente sin borrado por inactividad (idle delete TTL).")
                if active is not None and active < 60:
                    extras.append({"action": f"Convertir en cluster efímero / Dataproc Serverless: sólo está activo "
                                             f"~{active:.0f}% de las horas",
                                   "monthly_saving": round(current * (1 - active / 100), 2)})
            secondary = [g for g in groups if g["role"].startswith("secondary")]
            workers = next((g for g in groups if g["role"] == "worker"), None)
            if workers and not secondary and int(workers.get("count") or 0) >= 4:
                saving = P.node_group_monthly(workers["machine_type"], int(workers["count"]) // 2, False, True) * (1 - P.PREEMPTIBLE_DISCOUNT)
                extras.append({"action": "Mover ~50% de los workers a secondary workers Spot/preemptibles",
                               "monthly_saving": round(saving, 2)})
            if c.get("hdfs_heavy"):
                reasons.append("Usa HDFS en discos persistentes: el dato debería vivir en Cloud Storage (conector GCS) "
                               "para poder apagar el cluster sin perder información.")

    elif kind == "cloudsql":
        tier, ha = c.get("tier"), bool(c.get("high_availability"))
        current = P.cloudsql_monthly(tier, ha, float(c.get("disk_gb") or 0), c.get("disk_type") or "PD_SSD") or 0.0
        shape = P.cloudsql_tier_shape(tier)
        if verdict in ("oversized", "undersized") and shape and shape[0] > 0:
            ratio = peak / TARGET
            new_tier = P.cloudsql_tier_for(P.ceil_int(shape[0] * ratio), shape[1] / shape[0])
            if new_tier != tier:
                rec_cost = P.cloudsql_monthly(new_tier, ha, float(c.get("disk_gb") or 0), c.get("disk_type") or "PD_SSD")
                rec_desc = f"{tier} → {new_tier}"
            reasons.append(f"{_util_text(util)}.")
        elif verdict == "right_sized":
            reasons.append(f"{_util_text(util)}: el tier es adecuado.")
        else:
            reasons.append("Sin métricas de uso de Cloud SQL.")
        if ha and (c.get("environment") in ("dev", "test", "qa", "sandbox")):
            extras.append({"action": "Quitar alta disponibilidad en entorno no productivo",
                           "monthly_saving": round(current / 2, 2)})

    elif kind == "composer":
        size = c.get("environment_size") or "ENVIRONMENT_SIZE_SMALL"
        current = P.COMPOSER_ENV_MONTH.get(size, 400.0)
        dags = c.get("dag_count")
        if dags is not None and dags <= 5 and size != "ENVIRONMENT_SIZE_SMALL":
            verdict = "oversized"
            rec_desc, rec_cost = f"{size} → ENVIRONMENT_SIZE_SMALL", P.COMPOSER_ENV_MONTH["ENVIRONMENT_SIZE_SMALL"]
            reasons.append(f"Sólo {dags} DAGs para un entorno {size.split('_')[-1].lower()}.")
        if dags is not None and dags <= 3:
            reasons.append("Con tan pocos DAGs, Cloud Workflows + Cloud Scheduler o Dataform suelen ser mucho más baratos "
                           "que un entorno Composer permanente.")
        if verdict == "no_data":
            reasons.append("Costo de Composer estimado por tamaño de entorno (no incluye workers autoescalados).")
    else:
        current = float(c.get("monthly_cost_estimate") or 0)

    delta = round(rec_cost - current, 2) if rec_cost is not None else None
    return {
        "verdict": verdict,
        "verdict_es": {"oversized": "Sobredimensionado", "undersized": "Subdimensionado",
                       "right_sized": "Adecuado", "no_data": "Sin datos de uso"}[verdict],
        "reasons": reasons,
        "current_config": _describe(c.get("node_groups") or []) or c.get("tier") or c.get("environment_size"),
        "current_monthly_usd": round(current, 2),
        "recommended_config": rec_desc,
        "recommended_monthly_usd": round(rec_cost, 2) if rec_cost is not None else None,
        "monthly_delta_usd": delta,
        "additional_savings": extras,
        "total_potential_saving_usd": round(max(0.0, -(delta or 0)) + sum(e["monthly_saving"] for e in extras), 2),
        "explanation": (f"{'Ahorro' if (delta or 0) < 0 else 'Costo adicional'} estimado de {_fmt(abs(delta))}/mes "
                        f"al pasar de {_fmt(current)} a {_fmt(rec_cost)}.") if delta else None,
    }


def size_all(clusters: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for c in clusters:
        try:
            out.append({**c, "sizing": size_cluster(c)})
        except Exception as exc:  # noqa: BLE001
            out.append({**c, "sizing": {"verdict": "no_data", "verdict_es": "Error", "reasons": [str(exc)],
                                        "current_monthly_usd": None, "total_potential_saving_usd": 0}})
    return out
