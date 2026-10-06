"""Anexo técnico DETERMINISTA del informe de datos (Markdown generado en Python).

Garantiza que el detalle por proyecto → flujo (entradas, salidas, capas, modos, tiempos,
costos), el mapa medallion, el data lake y el dimensionamiento de clusters queden en el
informe con cifras exactas, aunque el LLM resuma o se corte por longitud.
"""
from __future__ import annotations

from collections import defaultdict
from typing import Any

MODE_ES = {"append": "append", "overwrite": "SOBRESCRIBE", "merge": "merge (upsert)", "update": "update/delete",
           "create": "crea", "unknown": "¿?", None: "—"}
STATE_ICON = {"active": "🟢", "inactive": "⚪", "failing": "🔴", "unknown": "❔"}


def _dur(s: float | None) -> str:
    if s is None:
        return "—"
    return f"{s / 3600:.1f} h" if s >= 3600 else f"{s / 60:.1f} min" if s >= 60 else f"{s:.0f} s"


def _money(x: float | None) -> str:
    return "—" if x is None else f"${x:,.2f}"


def _io(items: list[dict[str, Any]], out: bool = False, limit: int = 4) -> str:
    parts = []
    for i in items[:limit]:
        tag = f"[{i['layer']}]" if i.get("layer") else ""
        fmt = f" {i['format']}" if i.get("format") else ""
        mode = f" · {MODE_ES.get(i.get('write_mode'), i.get('write_mode'))}" if out and i.get("write_mode") else ""
        parts.append(f"`{i['name']}`{tag}{fmt}{mode}")
    more = f" +{len(items) - limit}" if len(items) > limit else ""
    return "<br>".join(parts) + more if parts else "—"


def build_appendix(raw: dict[str, Any], max_flow_cards: int = 40) -> str:
    dp = raw.get("data_platform") or {}
    flows = dp.get("flows") or []
    if not (flows or dp.get("tables") or dp.get("clusters")):
        return ""
    folders = {p["project_id"]: p.get("folder_path") for p in raw.get("projects") or []}
    lines = ["", "---", "", "# Anexo técnico (generado automáticamente a partir del inventario)", ""]

    # --- flujos por carpeta/proyecto
    lines.append("## A. Flujos y microservicios por proyecto")
    by_project: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for f in flows:
        by_project[f["project_id"]].append(f)
    cbs = {c["project_id"]: c for c in dp.get("cost_breakdown_by_project") or []}
    if not cbs and dp.get("cost_breakdown", {}).get("project_id"):
        cbs[dp["cost_breakdown"]["project_id"]] = dp["cost_breakdown"]
    for pid in sorted(by_project, key=lambda p: (folders.get(p) or "", p)):
        fs = sorted(by_project[pid], key=lambda f: -f["cost_30d_usd"])
        cb = cbs.get(pid, {})
        folder = f" · carpeta `{folders[pid]}`" if folders.get(pid) else ""
        lines += ["", f"### Proyecto `{pid}`{folder}",
                  f"{len(fs)} flujos · costo estimado de flujos {_money(cb.get('flows_total_usd'))}/mes · "
                  f"costos compartidos {_money(cb.get('shared_total_usd'))}/mes · **total {_money(cb.get('total_usd'))}/mes**", "",
                  "| Estado | Flujo | Motor | Entradas | Salidas (capa · modo) | Calendario | Duración media / p95 | Costo 30 d |",
                  "|---|---|---|---|---|---|---|---|"]
        for f in fs:
            lines.append(f"| {STATE_ICON.get(f['state'], '')} {f['state_es']} | **{f['name']}** | {f['engine']} | "
                         f"{_io(f['inputs'])} | {_io(f['outputs'], True)} | {f.get('schedule') or '—'} | "
                         f"{_dur(f.get('avg_duration_s'))} / {_dur(f.get('p95_duration_s'))} | {_money(f['cost_30d_usd'])} |")
        if cb.get("shared"):
            lines += ["", "Costos compartidos:", ""] + [f"- {s['resource']}: {_money(s['monthly_usd'])}/mes" for s in cb["shared"]]

    # --- grupos / cascadas
    cascades = dp.get("cascades") or []
    if cascades:
        lines += ["", "## A2. Grupos de flujos (cascadas)", "",
                  "| Grupo | Cómo se infirió | Flujos (en orden) | Capas recorridas | Owner(s) | Costo 30 d |",
                  "|---|---|---|---|---|---|"]
        for g in sorted(cascades, key=lambda x: (-x["num_flujos"], x["grupo_cascada_id"])):
            lines.append(f"| `{g['grupo_cascada_id']}` | {g['origen_inferencia']} | {g['flujos_en_orden']} | "
                         f"{g['capas_recorridas']} | {g['owners']} | {_money(g['costo_30d_usd'])} |")

    # --- fichas de flujos con problemas
    flagged = [f for f in flows if f.get("signals")]
    if flagged:
        lines += ["", "## B. Fichas de flujos con observaciones"]
        for f in sorted(flagged, key=lambda f: (-len(f["signals"]), -f["cost_30d_usd"]))[:max_flow_cards]:
            lines += ["", f"#### {f['name']}  ·  `{f['project_id']}`",
                      f"- **Motor:** {f['engine']} · **Identidad:** `{f.get('identity') or '—'}` · "
                      f"**Grupo:** `{f.get('grupo_cascada_id') or '—'}` (eslabón {f.get('orden_en_cascada') or '—'})",
                      f"- **Entradas:** {_io(f['inputs'], limit=10).replace('<br>', ', ')}",
                      f"- **Salidas:** {_io(f['outputs'], True, limit=10).replace('<br>', ', ')}",
                      f"- **Operación:** {f['state_es']} · {f.get('runs_in_window', 0)} ejecuciones · "
                      f"{f.get('failure_rate_pct') or 0}% con error · última {(f.get('last_run') or '—')[:16]} · "
                      f"media {_dur(f.get('avg_duration_s'))}, p95 {_dur(f.get('p95_duration_s'))}"
                      + (f", tendencia {f['duration_trend_pct']:+.0f}%" if f.get("duration_trend_pct") is not None else ""),
                      f"- **Costo estimado 30 días:** {_money(f['cost_30d_usd'])} ({', '.join(f.get('cost_basis') or ['sin base'])})",
                      "- **Observaciones:**"] + [f"  - [{s['severity']}] {s['text']}" for s in f["signals"][:8]]

    # --- medallion
    tables = dp.get("tables") or []
    if tables:
        lines += ["", "## C. Mapa medallion (BigQuery)", "",
                  "| Dataset | Capa | Tablas | Tamaño (GB) | Normalización predominante | Tablas con observaciones |",
                  "|---|---|---|---|---|---|"]
        by_ds: dict[tuple, list[dict[str, Any]]] = defaultdict(list)
        for t in tables:
            by_ds[(t["project_id"], t["dataset"])].append(t)
        for (pid, ds), ts in sorted(by_ds.items(), key=lambda kv: -sum(t["size_gb"] for t in kv[1]))[:60]:
            layers = sorted({t["layer"] or "sin capa" for t in ts})
            norm = max({t["normalization"] for t in ts}, key=lambda n: sum(1 for t in ts if t["normalization"] == n))
            lines.append(f"| `{pid}.{ds}` | {', '.join(layers)} | {len(ts)} | {sum(t['size_gb'] for t in ts):,.1f} | "
                         f"{norm} | {sum(1 for t in ts if t['signals'])} |")
        flagged_t = [t for t in tables if any(s["severity"] in ("HIGH", "MEDIUM") for s in t["signals"])]
        if flagged_t:
            lines += ["", "Tablas con observaciones relevantes:", "",
                      "| Tabla | Capa | GB | Normalización (esperada) | Observaciones |", "|---|---|---|---|---|"]
            for t in flagged_t[:60]:
                obs = "<br>".join(s["text"] for s in t["signals"] if s["severity"] in ("HIGH", "MEDIUM"))[:600]
                lines.append(f"| `{t['dataset']}.{t['table']}` | {t['layer'] or '—'} | {t['size_gb']:,.1f} | "
                             f"{t['normalization']} ({t['expected_for_layer']}) | {obs} |")

    # --- lake
    lake = dp.get("lake") or []
    if lake:
        lines += ["", "## D. Data lake (Cloud Storage)", "",
                  "| Bucket | Capa(s) | Clase | GB | Formatos (% bytes) | Archivos < 32 MB | Particionado | Observaciones |",
                  "|---|---|---|---|---|---|---|---|"]
        for b in lake:
            fmts = ", ".join(f"{k} {v:.0f}%" for k, v in list((b.get("formats_by_bytes") or {}).items())[:4]) or "—"
            layers = ", ".join(filter(None, [b.get("layer")] + b.get("layers_in_prefixes", []))) or "—"
            lines.append(f"| `{b['bucket']}` | {layers} | {b['storage_class']} | {b['size_gb']:,.1f} | {fmts} | "
                         f"{b.get('small_files_pct') if b.get('small_files_pct') is not None else '—'}% | "
                         f"{'sí' if b.get('partition_style') else 'no'} | "
                         f"{'<br>'.join(s['text'] for s in b['signals'])[:500] or '—'} |")

    # --- clusters
    clusters = dp.get("clusters") or []
    if clusters:
        lines += ["", "## E. Dimensionamiento de clusters", "",
                  "| Cluster | Tipo | Configuración actual | CPU p95 / Mem p95 | Veredicto | Recomendado | Costo actual | "
                  "Costo recomendado | Ahorro potencial total |", "|---|---|---|---|---|---|---|---|---|"]
        for c in clusters:
            s, u = c.get("sizing") or {}, c.get("utilization") or {}
            use = f"{u.get('cpu_p95', '—')}% / {u.get('mem_p95', '—')}%" if u else "sin datos"
            lines.append(f"| `{c['name']}` | {c['kind']} | {s.get('current_config') or '—'} | {use} | "
                         f"{s.get('verdict_es', '—')} | {s.get('recommended_config') or '—'} | "
                         f"{_money(s.get('current_monthly_usd'))} | {_money(s.get('recommended_monthly_usd'))} | "
                         f"{_money(s.get('total_potential_saving_usd'))} |")
        lines.append("")
        for c in clusters:
            s = c.get("sizing") or {}
            if s.get("reasons") or s.get("additional_savings"):
                lines.append(f"- **{c['name']}**: " + " ".join(s.get("reasons") or [])
                             + "".join(f" · {e['action']} (≈{_money(e['monthly_saving'])}/mes)"
                                       for e in s.get("additional_savings") or []))
    lines += ["", "_Costos estimados con precios de lista de GCP; los reales pueden variar por descuentos, "
                  "compromisos de uso y región. Con el export de facturación a BigQuery se muestra el costo real._"]
    return "\n".join(lines)
