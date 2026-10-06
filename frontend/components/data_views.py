"""Vistas de la plataforma de datos: flujos (por carpeta → proyecto → flujo), gobierno de datos, capacidad."""
from __future__ import annotations

from typing import Any

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from components.diagram import lineage_diagram

STATE_ICON = {"active": "🟢 activo", "inactive": "⚪ inactivo", "failing": "🔴 con fallos", "unknown": "❔ desconocido"}
MODE_ES = {"overwrite": "SOBRESCRIBE", "append": "append", "merge": "merge", "update": "update", "create": "crea"}
VERDICT_ICON = {"oversized": "🔻 Sobredimensionado", "undersized": "🔺 Subdimensionado", "right_sized": "✅ Adecuado",
                "no_data": "❔ Sin datos de uso"}


def _dur(s: float | None) -> str:
    if s is None:
        return "—"
    return f"{s / 3600:.1f} h" if s >= 3600 else f"{s / 60:.1f} min" if s >= 60 else f"{s:.0f} s"


def _io(items: list[dict[str, Any]], out: bool = False) -> str:
    return " · ".join(f"{i['name']}" + (f" [{i['layer']}]" if i.get("layer") else "")
                      + (f" ({MODE_ES.get(i.get('write_mode'), i.get('write_mode'))})" if out and i.get("write_mode") else "")
                      for i in items[:4]) + (f" +{len(items) - 4}" if len(items) > 4 else "")


# ============================================================================ flujos
def show_flows(inv: dict[str, Any]) -> None:
    dp = inv.get("data_platform") or {}
    flows = dp.get("flows") or []
    if not flows:
        st.info("No se detectaron flujos. Revisa en las advertencias los permisos de BigQuery (historial de jobs), "
                "Dataflow, Dataproc y Composer, o carga un catálogo de flujos en el panel lateral.")
        return
    folders = {p["project_id"]: p.get("folder_path") for p in inv.get("projects") or []}
    for f in flows:
        f.setdefault("folder_path", folders.get(f["project_id"]))
    total = sum(f["cost_30d_usd"] for f in flows)
    c = st.columns(5)
    c[0].metric("Flujos", len(flows))
    c[1].metric("Activos", sum(f["state"] == "active" for f in flows))
    c[2].metric("Inactivos", sum(f["state"] == "inactive" for f in flows))
    c[3].metric("Con fallos", sum(f["state"] == "failing" for f in flows))
    c[4].metric("Costo estimado 30 días", f"${total:,.2f}")

    st.subheader("Costos por carpeta → proyecto → flujo")
    df = pd.DataFrame([{"Carpeta": f.get("folder_path") or "(proyecto)", "Proyecto": f["project_id"],
                        "Tipo": f["engine"], "Flujo": f["name"], "Costo": max(f["cost_30d_usd"], 0.01),
                        "Costo real": f["cost_30d_usd"]} for f in flows])
    fig = px.treemap(df, path=["Carpeta", "Proyecto", "Tipo", "Flujo"], values="Costo",
                     hover_data={"Costo real": ":$,.2f", "Costo": False})
    fig.update_traces(texttemplate="%{label}<br>$%{value:,.2f}")
    fig.update_layout(height=420, margin=dict(l=0, r=0, t=10, b=0))
    st.plotly_chart(fig, width="stretch")

    cb = dp.get("cost_breakdown_by_project") or ([dp["cost_breakdown"]] if dp.get("cost_breakdown", {}).get("project_id") else [])
    if cb:
        st.dataframe(pd.DataFrame([{"Carpeta": c.get("folder_path") or "—", "Proyecto": c["project_id"],
                                    "Flujos ($/mes)": c["flows_total_usd"], "Compartidos ($/mes)": c["shared_total_usd"],
                                    "Total ($/mes)": c["total_usd"],
                                    "Compartidos": " · ".join(f"{s['resource']}: ${s['monthly_usd']:,.2f}" for s in c.get("shared") or [])}
                                   for c in cb]), width="stretch", hide_index=True)

    cascades = dp.get("cascades") or []
    if cascades:
        st.subheader("Grupos de flujos (cascadas)")
        st.dataframe(pd.DataFrame([{"Grupo": g["grupo_cascada_id"], "Cómo se infirió": g["origen_inferencia"],
                                    "Flujos en orden": g["flujos_en_orden"], "Capas": g["capas_recorridas"],
                                    "Owner(s)": g["owners"], "Flujos": g["num_flujos"], "Cascada": "sí" if g["es_cascada"] else "no",
                                    "Costo 30 d ($)": g["costo_30d_usd"]} for g in cascades]),
                     width="stretch", hide_index=True,
                     column_config={"Costo 30 d ($)": st.column_config.NumberColumn(format="$%.2f")})

    st.subheader("Linaje por capas")
    lineage_diagram(dp, key="flows_lin")

    st.subheader("Catálogo de flujos")
    fc = st.columns(4)
    projects = sorted({f["project_id"] for f in flows})
    sel_p = fc[0].multiselect("Proyecto", projects, default=projects, key="fl_p")
    states = sorted({f["state"] for f in flows})
    sel_s = fc[1].multiselect("Estado", states, default=states, key="fl_s")
    engines = sorted({f["engine"] for f in flows})
    sel_e = fc[2].multiselect("Motor", engines, default=engines, key="fl_e")
    text = fc[3].text_input("Buscar (nombre, tabla…)", key="fl_q").lower()
    view = [f for f in flows if f["project_id"] in sel_p and f["state"] in sel_s and f["engine"] in sel_e
            and (not text or text in f["name"].lower()
                 or any(text in x["name"].lower() for x in f["inputs"] + f["outputs"]))]
    table = pd.DataFrame([{"Estado": STATE_ICON.get(f["state"], f["state"]), "Grupo": f.get("grupo_cascada_id") or "—",
                           "Eslabón": f.get("orden_en_cascada"), "Flujo": f["name"], "Proyecto": f["project_id"],
                           "Capas": f"{f.get('capa_entrada_inferida') or '—'} → {f.get('capa_salida_inferida') or '—'}",
                           "Motor": f["engine"], "Entradas": _io(f["inputs"]), "Salidas (capa · modo)": _io(f["outputs"], True),
                           "Calendario": f.get("schedule") or "—", "Ejecuciones": f.get("runs_in_window"),
                           "Última": (f.get("last_run") or "—")[:16], "Media": _dur(f.get("avg_duration_s")),
                           "p95": _dur(f.get("p95_duration_s")), "Costo 30 d ($)": f["cost_30d_usd"],
                           "Observaciones": len(f.get("signals") or [])} for f in view])
    st.dataframe(table, width="stretch", hide_index=True,
                 column_config={"Costo 30 d ($)": st.column_config.NumberColumn(format="$%.2f")})

    names = {f"{f['name']} · {f['project_id']}": f for f in view}
    if names:
        pick = st.selectbox("Detalle del flujo", list(names), key="fl_pick")
        flow_detail(names[pick])

    conflicts = [i for i in dp.get("issues") or [] if i["code"] in ("FLOW_CONFLICT", "FLOW_CYCLE", "REDUNDANT_FLOW")]
    if conflicts:
        st.subheader("Contradicciones y dependencias entre flujos")
        by_id = {f["resource_id"]: f["name"] for f in flows}
        st.dataframe(pd.DataFrame([{"Severidad": i["severity"], "Tipo": i["code"], "Detalle": i["text"],
                                    "Flujos": ", ".join(by_id.get(x, x) for x in i["flows"])} for i in conflicts]),
                     width="stretch", hide_index=True)


def flow_detail(f: dict[str, Any]) -> None:
    c = st.columns(4)
    c[0].metric("Estado", STATE_ICON.get(f["state"], f["state"]))
    c[1].metric("Duración media / p95", f"{_dur(f.get('avg_duration_s'))} / {_dur(f.get('p95_duration_s'))}",
                f"{f['duration_trend_pct']:+.0f}% tendencia" if f.get("duration_trend_pct") is not None else None,
                delta_color="inverse")
    c[2].metric("Ejecuciones / fallos", f"{f.get('runs_in_window') or 0} / {f.get('failures') or 0}")
    c[3].metric("Costo 30 días", f"${f['cost_30d_usd']:,.2f}", help=", ".join(f.get("cost_basis") or []) or None)
    st.caption(f"Motor: {f['engine']} · Identidad: {f.get('identity') or '—'} · Calendario: {f.get('schedule') or '—'}"
               + (f" · Dueño: {f['owner']}" if f.get("owner") else "") + (f" · {f['description']}" if f.get("description") else ""))
    a, b = st.columns(2)
    a.markdown("**Entradas**")
    a.dataframe(pd.DataFrame([{"Sistema": i.get("system"), "Dato": i["name"], "Capa": i.get("layer") or "—",
                               "Formato": i.get("format") or "—", "Estructura": i.get("structure") or "—"}
                              for i in f["inputs"]]) if f["inputs"] else pd.DataFrame({"Entradas": ["(no detectadas)"]}),
                width="stretch", hide_index=True)
    b.markdown("**Salidas**")
    b.dataframe(pd.DataFrame([{"Sistema": o.get("system"), "Dato": o["name"], "Capa": o.get("layer") or "—",
                               "Modo de escritura": MODE_ES.get(o.get("write_mode"), o.get("write_mode") or "—")}
                              for o in f["outputs"]]) if f["outputs"] else pd.DataFrame({"Salidas": ["(no detectadas)"]}),
                width="stretch", hide_index=True)
    runs = [r for r in f.get("runs") or [] if r.get("duration_s") is not None]
    if runs:
        fig = go.Figure(go.Bar(x=[r["start"][:16] for r in runs], y=[r["duration_s"] / 60 for r in runs],
                               marker_color=["#34A853" if r["ok"] else "#D93025" for r in runs]))
        fig.update_layout(title="Duración de las últimas ejecuciones (min)", height=240, margin=dict(l=10, r=10, t=40, b=10))
        st.plotly_chart(fig, width="stretch")
    for s in f.get("signals") or []:
        (st.error if s["severity"] in ("CRITICAL", "HIGH") else st.warning if s["severity"] == "MEDIUM" else st.info)(s["text"])
    if f.get("sample_query"):
        with st.expander("Consulta de ejemplo"):
            st.code(f["sample_query"], language="sql")


# ============================================================================ gobierno de datos
def show_data(inv: dict[str, Any]) -> None:
    dp = inv.get("data_platform") or {}
    tables, lake, datasets = dp.get("tables") or [], dp.get("lake") or [], dp.get("datasets") or []
    if not (tables or lake):
        st.info("Sin tablas de BigQuery ni buckets analizados (revisa permisos de BigQuery y Storage en las advertencias).")
        return
    s = dp.get("summary") or {}
    c = st.columns(5)
    c[0].metric("Tablas", len(tables))
    c[1].metric("Con capa identificada", f"{s.get('pct_tables_with_layer') or 0}%")
    c[2].metric("Tablas grandes particionadas", f"{s.get('pct_large_tables_partitioned') if s.get('pct_large_tables_partitioned') is not None else '—'}%")
    c[3].metric("Tablas con posible PII", s.get("tables_with_pii", 0))
    c[4].metric("Buckets del lake", len(lake))

    if tables:
        order = ["landing", "bronze", "silver", "gold", "sin capa"]
        agg = pd.DataFrame([{"Capa": t["layer"] or "sin capa", "GB": t["size_gb"]} for t in tables]) \
            .groupby("Capa").agg(Tablas=("GB", "size"), GB=("GB", "sum")).reindex(order).dropna().reset_index()
        fig = go.Figure([go.Bar(name="Tablas", x=agg["Capa"], y=agg["Tablas"], marker_color="#4285F4"),
                         go.Bar(name="GB", x=agg["Capa"], y=agg["GB"], marker_color="#FBBC04", yaxis="y2")])
        fig.update_layout(title="Arquitectura medallion", barmode="group", height=300,
                          yaxis2={"overlaying": "y", "side": "right", "title": "GB"}, margin=dict(l=10, r=10, t=40, b=10))
        st.plotly_chart(fig, width="stretch")

    if datasets:
        st.subheader("Datasets")
        st.dataframe(pd.DataFrame([{"Dataset": f"{d['project_id']}.{d['dataset']}", "Capa": d.get("layer") or "—",
                                    "Capas de sus tablas": ", ".join(d.get("layers_mixed") or []) or "—",
                                    "Tablas": d["tables"], "GB": d["size_gb"], "Facturación": d["storage_billing_model"],
                                    "Recomendado": d["recommended_billing_model"],
                                    "Ahorro/mes": d["billing_model_saving_usd"]} for d in datasets]),
                     width="stretch", hide_index=True)

    if tables:
        st.subheader("Tablas: capa, normalización y esquema")
        fc = st.columns(3)
        layers = sorted({t["layer"] or "sin capa" for t in tables})
        sel = fc[0].multiselect("Capa", layers, default=layers, key="dt_l")
        codes = sorted({x["code"] for t in tables for x in t["signals"]})
        sel_c = fc[1].multiselect("Observación", codes, key="dt_c")
        only = fc[2].toggle("Sólo con observaciones MEDIUM/HIGH", value=True, key="dt_o")
        rows = [t for t in tables if (t["layer"] or "sin capa") in sel
                and (not sel_c or any(x["code"] in sel_c for x in t["signals"]))
                and (not only or any(x["severity"] in ("HIGH", "MEDIUM") for x in t["signals"]))]
        st.dataframe(pd.DataFrame([{"Tabla": f"{t['dataset']}.{t['table']}", "Capa": t["layer"] or "—",
                                    "Cómo se determinó": t["layer_source"], "GB": t["size_gb"],
                                    "Particionada": "sí" if t["partitioned"] else "no",
                                    "Normalización": t["normalization"], "Esperada en la capa": t["expected_for_layer"],
                                    "Escrita por": len(t["writers"]), "Leída por": len(t["readers"]),
                                    "Consultas 30 d": t["consumer_queries_30d"],
                                    "Observaciones": " | ".join(x["text"] for x in t["signals"])} for t in rows[:500]]),
                     width="stretch", hide_index=True)

    if lake:
        st.subheader("Data lake (Cloud Storage)")
        st.dataframe(pd.DataFrame([{"Bucket": b["bucket"], "Capa(s)": ", ".join(filter(None, [b.get("layer")] + b.get("layers_in_prefixes", []))) or "—",
                                    "Clase": b["storage_class"], "GB": b["size_gb"], "$/mes": b["monthly_usd"],
                                    "Formatos (% bytes)": ", ".join(f"{k} {v:.0f}%" for k, v in (b.get("formats_by_bytes") or {}).items()),
                                    "Archivos < 32 MB": b.get("small_files_pct"), "Tamaño medio MB": b.get("avg_object_mb"),
                                    "Particionado": "sí" if b.get("partition_style") else "no",
                                    "Antigüedad mediana (días)": b.get("median_age_days"),
                                    "Ciclo de vida": b.get("lifecycle_rules"),
                                    "Observaciones": " | ".join(x["text"] for x in b["signals"])} for b in lake]),
                     width="stretch", hide_index=True)


# ============================================================================ capacidad
def show_capacity(inv: dict[str, Any]) -> None:
    clusters = (inv.get("data_platform") or {}).get("clusters") or []
    if not clusters:
        st.info("No se encontraron clusters Dataproc, GKE, Cloud SQL ni entornos Composer.")
        return
    sized = [c.get("sizing") or {} for c in clusters]
    c = st.columns(4)
    c[0].metric("Clusters / instancias", len(clusters))
    c[1].metric("Sobredimensionados", sum(s.get("verdict") == "oversized" for s in sized))
    c[2].metric("Subdimensionados", sum(s.get("verdict") == "undersized" for s in sized))
    c[3].metric("Ahorro potencial / mes", f"${sum(s.get('total_potential_saving_usd') or 0 for s in sized):,.2f}")

    names = [c["name"] for c in clusters]
    fig = go.Figure([go.Bar(name="Actual", x=names, y=[s.get("current_monthly_usd") or 0 for s in sized], marker_color="#EA4335"),
                     go.Bar(name="Recomendado", x=names,
                            y=[s.get("recommended_monthly_usd") if s.get("recommended_monthly_usd") is not None
                               else s.get("current_monthly_usd") or 0 for s in sized], marker_color="#34A853")])
    fig.update_layout(title="Costo mensual estimado: actual vs recomendado", barmode="group", height=320,
                      margin=dict(l=10, r=10, t=40, b=10))
    st.plotly_chart(fig, width="stretch")

    for cl, s in zip(clusters, sized):
        u = cl.get("utilization") or {}
        head = (f"{VERDICT_ICON.get(s.get('verdict'), '')} · **{cl['name']}** ({cl['kind']}) · "
                f"${s.get('current_monthly_usd') or 0:,.2f}/mes")
        with st.expander(head.replace("**", ""), expanded=s.get("verdict") in ("oversized", "undersized")):
            a, b, d = st.columns(3)
            a.metric("CPU p95 / media", f"{u.get('cpu_p95', '—')}% / {u.get('cpu_avg', '—')}%" if u else "sin datos")
            b.metric("Memoria p95 / media", f"{u.get('mem_p95', '—')}% / {u.get('mem_avg', '—')}%" if u else "sin datos")
            d.metric("Horas activas", f"{u.get('active_hours_pct', '—')}%" if u else "—")
            st.markdown(f"**Configuración actual:** {s.get('current_config') or '—'}  \n"
                        f"**Recomendación:** {s.get('recommended_config') or 'mantener'}"
                        + (f"  \n**{s['explanation']}**" if s.get("explanation") else ""))
            for r in s.get("reasons") or []:
                st.write("•", r)
            for e in s.get("additional_savings") or []:
                st.success(f"{e['action']} → ahorro estimado ${e['monthly_saving']:,.2f}/mes")
