"""Frontend Streamlit — Cloud Tech Debt Analyzer.

Ejecutar:  streamlit run frontend/app.py
Requiere el backend activo (uvicorn backend.main:app --port 8000).
"""
from __future__ import annotations

import json
import os
from typing import Any

import pandas as pd
import plotly.graph_objects as go
import requests
import streamlit as st

from components.data_views import show_capacity, show_data, show_flows
from components.diagram import render_markdown, services_diagram

API_URL = os.getenv("BACKEND_URL", "http://localhost:8000")

NODE_LABELS = {
    "discovery": "🔎 Descubrimiento (infraestructura, arquitectura y plataforma de datos)",
    "security_analyzer": "🛡️ Agente de Seguridad",
    "governance_analyzer": "📋 Agente de Gobierno",
    "cost_analyzer": "💰 Agente de Costos",
    "data_governance_analyzer": "🗄️ Agente de Gobierno de Datos",
    "pipeline_analyzer": "🔄 Agente de Flujos",
    "capacity_analyzer": "🖥️ Agente de Capacidad",
    "kpi_calculator": "📊 Cálculo de KPIs",
    "report_generator": "📝 Informe ejecutivo",
    "data_report_generator": "📚 Informe de datos",
    "excel_exporter": "📗 Exportación a Excel (inventario, dependencias, linaje)",
}
CATALOG_TEMPLATE = """# Catálogo de flujos externos o no visibles desde GCP (opcional).
# Se combina con lo que el auditor descubre automáticamente.
flows:
  - name: ingesta_ventas_hdfs
    project: mi-proyecto            # opcional: proyecto al que pertenece
    engine: Spark on-premise
    owner: equipo-datos@empresa.com
    schedule: "0 2 * * *"
    description: Copia diaria de ventas desde el clúster Hadoop al lake
    inputs:
      - name: hdfs:///data/ventas/
        system: hdfs
        layer: source
        format: orc
        structure: "venta_id INT, fecha STRING, cliente_id INT, total DECIMAL"
      - name: jdbc:postgresql://pg-prod.empresa.local:5432/erp
        system: postgres
    outputs:
      - name: gs://lake-bronze/ventas/
        layer: bronze
        format: parquet
        write_mode: append        # append | overwrite | merge
"""

SCOPES = {"Organización": "organization", "Carpeta": "folder", "Proyecto / Cuenta": "project"}
LLMS = {"OpenAI": "openai", "Anthropic (Claude)": "anthropic", "Google Gemini": "gemini"}
TARGET_HELP = {
    ("AWS", "organization"): ("Organization ID", "o-a1b2c3d4e5"),
    ("AWS", "folder"): ("Organizational Unit ID", "ou-ab12-cdef3456"),
    ("AWS", "project"): ("Account ID (12 dígitos)", "123456789012"),
    ("GCP", "organization"): ("Organization ID", "123456789012"),
    ("GCP", "folder"): ("Folder ID", "987654321098"),
    ("GCP", "project"): ("Project ID", "mi-proyecto-prod"),
    ("Azure", "organization"): ("Tenant ID", "00000000-0000-0000-0000-000000000000"),
    ("Azure", "folder"): ("Management Group ID", "mg-plataforma"),
    ("Azure", "project"): ("Subscription ID", "00000000-0000-0000-0000-000000000000"),
}
SEV_COLORS = {"CRITICAL": "#b91c1c", "HIGH": "#ea580c", "MEDIUM": "#ca8a04", "LOW": "#2563eb"}

st.set_page_config(page_title="Cloud Tech Debt Analyzer", page_icon="☁️", layout="wide")
st.markdown("""<style>
.kpi-card{border:1px solid rgba(128,128,128,.25);border-radius:12px;padding:14px 16px;margin-bottom:8px}
.kpi-card h4{margin:0 0 6px 0;font-size:.95rem;opacity:.8}
.kpi-row{display:flex;justify-content:space-between;font-size:.9rem;padding:2px 0}
</style>""", unsafe_allow_html=True)


# ---------------------------------------------------------------- Sidebar
@st.cache_data(ttl=20, show_spinner=False)
def backend_health() -> dict[str, Any]:
    return requests.get(f"{API_URL}/health", timeout=3).json()


def sidebar() -> dict[str, Any] | None:
    with st.sidebar:
        st.header("⚙️ Configuración de la auditoría")
        try:
            h = backend_health()
            mode = h.get("discovery_mode", "desconocido (backend antiguo)")
            if h.get("missing_gcp_modules"):
                st.error("El backend no tiene instaladas las librerías de GCP. En su entorno "
                         f"(`{h.get('python')}`) ejecuta: `pip install -r requirements.txt` y reinícialo.")
            elif mode == "real":
                st.success(f"Backend v{h.get('version', '?')} · descubrimiento REAL (GCP)")
            else:
                st.warning(f"Backend v{h.get('version', '1.0.0')} · descubrimiento: {mode}. "
                           "Los datos serán SIMULADOS. Reinicia el backend con la versión nueva y sin DISCOVERY_MODE=mock.")
        except requests.RequestException:
            st.error(f"Backend no disponible en {API_URL}")
        st.subheader("1. Modelo de IA")
        llm_label = st.selectbox("Proveedor del modelo", list(LLMS))
        api_key = st.text_input("LLM API Key *", type="password",
                                help="Se envía sólo al backend para esta auditoría; no se almacena.")
        llm_model = st.text_input("Modelo (opcional)", placeholder="Por defecto según proveedor")

        st.subheader("2. Entorno cloud")
        provider = st.selectbox("Cloud Provider *", ["AWS", "GCP", "Azure"])
        scope_label = st.selectbox("Alcance *", list(SCOPES))
        scope = SCOPES[scope_label]
        label, placeholder = TARGET_HELP[(provider, scope)]
        target_id = st.text_input(f"{label} *", placeholder=placeholder)

        st.subheader("3. Credenciales cloud (opcional)")
        cred_mode = st.radio("Tipo", ["Ninguna (modo demo)", "Archivo JSON", "Token de sesión"], horizontal=False)
        credentials: str | None = None
        if cred_mode == "Archivo JSON":
            up = st.file_uploader("Archivo de credenciales", type=["json"])
            if up is not None:
                try:
                    credentials = json.dumps(json.load(up))
                    st.success("JSON válido cargado.")
                except json.JSONDecodeError:
                    st.error("El archivo no es un JSON válido.")
        elif cred_mode == "Token de sesión":
            credentials = st.text_input("Token", type="password") or None
        st.subheader("4. Facturación (opcional)")
        billing_table = st.text_input("Tabla del export de facturación (BigQuery)",
                                      placeholder="mi-proyecto.billing_export.gcp_billing_export_v1_XXXX",
                                      help="Si exportas la facturación a BigQuery, se calcula el costo real de los "
                                           "últimos 30 días por servicio. Sin esto se revisa cuenta de facturación y presupuestos.")
        st.subheader("5. Plataforma de datos")
        max_projects = st.slider("Máximo de proyectos (carpeta/organización)", 1, 50, 10) if scope != "project" else 1
        history_days = st.slider("Días de historial de flujos", 7, 90, 30,
                                 help="Ventana para tiempos de ejecución, inactividad y costos de BigQuery/Dataflow/Dataproc.")
        sample_lake = st.checkbox("Muestrear archivos del data lake (GCS)", value=True)
        read_dags = st.checkbox("Leer el código de los DAGs de Composer", value=True)
        catalog_file = st.file_uploader("Catálogo de flujos externos (YAML/JSON, opcional)", type=["yaml", "yml", "json"],
                                        help="Para fuentes que GCP no ve: HDFS on-premise, Postgres externos, SFTP… "
                                             "Describe entradas, capas, formatos, estructura y modo de escritura.")
        catalog_text = catalog_file.getvalue().decode("utf-8", "replace") if catalog_file else None
        st.download_button("⬇️ Plantilla de catálogo", CATALOG_TEMPLATE, "catalogo_flujos.yaml", "text/yaml",
                           width="stretch")
        st.caption("ℹ️ GCP + Proyecto: sube el JSON de una Service Account de sólo lectura. "
                   "El archivo se usa en un temporal y se borra al terminar.")

        st.divider()
        start = st.button("🚀 Iniciar Auditoría", type="primary", width="stretch")

    if not start:
        return None
    missing = [n for n, v in (("API Key", api_key), (label, target_id)) if not v.strip()]
    if missing:
        st.sidebar.error("Campos obligatorios: " + ", ".join(missing))
        return None
    return {"api_key": api_key.strip(), "body": {
        "provider": provider, "scope": scope, "target_id": target_id.strip(),
        "llm_provider": LLMS[llm_label], "llm_model": llm_model.strip() or None, "cloud_credentials": credentials,
        "billing_export_table": billing_table.strip() or None, "history_days": history_days,
        "sample_lake": sample_lake, "read_dags": read_dags, "max_projects": max_projects, "flow_catalog": catalog_text}}


# ---------------------------------------------------------------- Ejecución
def start_audit(api_key: str, body: dict[str, Any]) -> str | None:
    """Lanza la auditoría en segundo plano en el backend y devuelve su job_id."""
    try:
        r = requests.post(f"{API_URL}/api/v1/audits", json=body, timeout=30, headers={"X-LLM-API-Key": api_key})
    except requests.RequestException:
        st.error(f"No se pudo conectar con el backend en {API_URL}. ¿Está en ejecución?")
        return None
    if r.status_code != 202:
        try:
            detail = r.json().get("detail", r.text)
        except ValueError:
            detail = r.text
        st.error(f"Error {r.status_code}: {detail}")
        return None
    return r.json()["job_id"]


def render_progress(events: list[dict[str, Any]], elapsed: float) -> None:
    done = {e["node"]: e for e in events}
    st.progress(len(done) / len(NODE_LABELS), text=f"{len(done)}/{len(NODE_LABELS)} etapas · {elapsed:.0f}s")
    analyzers = {"security_analyzer", "governance_analyzer", "cost_analyzer", "data_governance_analyzer",
                 "pipeline_analyzer", "capacity_analyzer"}
    for node, lbl in NODE_LABELS.items():
        if node in done:
            errs = done[node]["summary"]["errors"]
            extra = f" — {done[node]['summary']['findings']} hallazgos" if node in analyzers else ""
            st.markdown(("🟠 " if errs else "🟢 ") + lbl + extra)
            for e in errs[:6]:
                st.caption(f"⚠️ {e[:300]}")
        else:
            running = (node == "discovery" or (node in analyzers and "discovery" in done)
                       or (node == "kpi_calculator" and analyzers <= done.keys())
                       or (node in ("report_generator", "data_report_generator") and "kpi_calculator" in done)
                       or (node == "excel_exporter" and {"report_generator", "data_report_generator"} <= done.keys()))
            st.markdown(("🔵 " if running else "⚪ ") + lbl + (" — en curso…" if running else ""))


@st.fragment(run_every=2)
def poll_job() -> None:
    """Consulta el estado cada 2 s sin bloquear la página. Al terminar, recarga para mostrar resultados."""
    job_id = st.session_state.get("job_id")
    if not job_id:
        return
    st.subheader("⏳ Progreso del análisis")
    try:
        job = requests.get(f"{API_URL}/api/v1/audits/{job_id}", timeout=10).json()
    except requests.RequestException:
        st.warning("Sin respuesta del backend; reintentando…")
        return
    if "status" not in job:
        st.error(job.get("detail", "Auditoría no encontrada."))
        st.session_state.pop("job_id", None)
        return
    render_progress(job["events"], job.get("elapsed_s", 0))
    if job["status"] == "completed":
        st.session_state["result"] = job["result"]
        st.session_state.pop("job_id", None)
        st.rerun()
    elif job["status"] == "error":
        st.error(f"El análisis falló: {job['error']}")
        st.session_state.pop("job_id", None)


# ---------------------------------------------------------------- Resultados
def excel_download(result: dict[str, Any]) -> None:
    """Botón para descargar el Excel generado por el nodo excel_exporter del backend (se conserva 24 h)."""
    rep = result.get("excel_report") or {}
    rid = rep.get("report_id")
    if not rid:
        return
    cache = st.session_state.setdefault("excel_cache", {})
    if rid not in cache:
        try:
            r = requests.get(f"{API_URL}/api/v1/reports/{rid}.xlsx", timeout=30)
            cache[rid] = r.content if r.status_code == 200 else None
        except requests.RequestException:
            cache[rid] = None
    sheets = ", ".join(f"{k} ({v})" for k, v in (rep.get("sheets") or {}).items())
    if cache[rid]:
        st.download_button("📗 Descargar Excel de linaje", cache[rid], file_name=rep.get("filename") or "linaje.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", help=sheets)
    else:
        st.caption("El Excel ya no está disponible en el backend (se conserva 24 h o el backend se reinició).")


def gauge(value: float, title: str) -> go.Figure:
    fig = go.Figure(go.Indicator(
        mode="gauge+number", value=value, title={"text": title, "font": {"size": 15}},
        number={"suffix": "/100"},
        gauge={"axis": {"range": [0, 100]}, "bar": {"color": "#334155"},
               "steps": [{"range": [0, 30], "color": "#bbf7d0"}, {"range": [30, 60], "color": "#fef08a"},
                         {"range": [60, 100], "color": "#fecaca"}]}))
    fig.update_layout(height=210, margin=dict(l=20, r=20, t=50, b=10))
    return fig


def kpi_card(title: str, rows: list[tuple[str, str]]) -> None:
    body = "".join(f"<div class='kpi-row'><span>{k}</span><b>{v}</b></div>" for k, v in rows)
    st.markdown(f"<div class='kpi-card'><h4>{title}</h4>{body}</div>", unsafe_allow_html=True)


def show_results(result: dict[str, Any]) -> None:
    k = result.get("kpis") or {}
    meta = (result.get("raw_infra_data") or {}).get("metadata", {})
    status = result.get("discovery_status") or ("mock" if meta.get("collector", "").startswith("mock") else "desconocido")
    source = f"Origen del inventario: **{meta.get('collector', 'mock')}** · estado: **{status}** · proyecto: `{result.get('target_id')}`"
    if status in ("complete",):
        st.success(source)
    elif status == "partial":
        st.warning(source + " — algunas secciones fallaron (ver advertencias).")
    elif status == "failed":
        st.error(source + " — no se pudo leer el proyecto:")
        for e in (result.get("errors") or []):
            if e.startswith("discovery:"):
                st.code(e.removeprefix("discovery: "), language=None)
    elif status == "desconocido":
        st.info(source)
    else:
        st.error(source + " — ⚠️ ESTOS DATOS SON SIMULADOS, no corresponden a tu proyecto.")
    inv = result.get("raw_infra_data") or {}
    if inv.get("asset_counts"):
        st.caption("Servicios detectados: " + " · ".join(f"{t}: {n}" for t, n in inv["asset_counts"].items()
                                                         if t != "API habilitada"))
    elif inv.get("storage_buckets") is not None:
        st.caption("Recursos leídos: " + " · ".join(f"{k.replace('_', ' ')}: {len(v)}" for k, v in inv.items()
                                                      if isinstance(v, list)))
    excel_download(result)
    if result.get("errors"):
        with st.expander(f"⚠️ {len(result['errors'])} advertencias durante el análisis"):
            for e in result["errors"]:
                st.write("•", e)

    (tab_kpi, tab_arch, tab_flows, tab_data, tab_cap, tab_ids, tab_bill, tab_report, tab_dreport, tab_findings,
     tab_raw) = st.tabs(["📊 KPIs", "🕸️ Arquitectura", "🔄 Flujos", "🗄️ Datos", "🖥️ Capacidad", "🔐 Identidades",
                         "💳 Facturación", "📝 Informe", "📚 Informe de datos", "🔍 Hallazgos", "🗂️ Inventario"])
    with tab_arch:
        services_diagram(inv)
    with tab_flows:
        show_flows(inv)
    with tab_data:
        show_data(inv)
    with tab_cap:
        show_capacity(inv)
    with tab_dreport:
        drep = result.get("data_report") or "_Sin informe de datos._"
        render_markdown(drep)
        st.download_button("⬇️ Descargar informe de datos (.md)", drep,
                           file_name=f"informe_datos_{result['provider']}_{result['target_id']}.md", mime="text/markdown")
    with tab_ids:
        show_identities(inv)
    with tab_bill:
        show_billing(inv)
    with tab_kpi:
        if not k:
            st.info("No hay KPIs disponibles.")
        else:
            c = st.columns(4)
            c[0].metric("Deuda técnica global", f"{k['overall_debt_score']}/100", help="0 = sin deuda, 100 = deuda máxima")
            c[1].metric("Calificación", k["grade"])
            c[2].metric("Hallazgos", k["total_findings"])
            c[3].metric("Ahorro potencial / mes", f"${k['cost']['monthly_waste_usd']:,.2f}",
                        help="Suma del desperdicio estimado de todas las categorías (costos, capacidad, flujos y almacenamiento).")

            cats = [("security", "Seguridad"), ("governance", "Gobierno"), ("cost", "Costos"),
                    ("data_governance", "Gobierno de datos"), ("pipelines", "Flujos"), ("capacity", "Capacidad")]
            cats = [(key, name) for key, name in cats if key in k["debt_scores"]]
            for row in range(0, len(cats), 3):
                g = st.columns(3)
                for col, (key, name) in zip(g, cats[row:row + 3]):
                    col.plotly_chart(gauge(k["debt_scores"][key], name), width="stretch", key=f"gauge_{key}")

            s, gv, cs = k["security"], k["governance"], k["cost"]
            c1, c2, c3 = st.columns(3)
            with c1:
                kpi_card("🛡️ Seguridad", [("Roles con admin", f"{s['pct_admin_roles']}%"),
                                          ("Roles con comodines", f"{s['pct_wildcard_roles']}%"),
                                          ("Buckets públicos", f"{s['pct_public_buckets']}%"),
                                          ("Instancias expuestas", f"{s['pct_public_instances']}%"),
                                          ("Servicios públicos", f"{s.get('pct_public_workloads', 0)}%"),
                                          ("Workloads con SA por defecto", f"{s.get('pct_workloads_default_sa', 0)}%"),
                                          ("Identidades sobreprivilegiadas", f"{s.get('pct_overprivileged_identities', 0)}%"),
                                          ("Secretos en variables de entorno", s.get("secrets_in_env", 0))])
            with c2:
                kpi_card("📋 Gobierno", [("Recursos sin etiquetas", f"{gv['pct_untagged_resources']}%"),
                                        ("Buckets sin cifrar", f"{gv['pct_unencrypted_buckets']}%"),
                                        ("Discos sin cifrar", f"{gv['pct_unencrypted_disks']}%"),
                                        ("Bases de datos sin backup", f"{gv.get('pct_db_without_backup', 0)}%")])
            with c3:
                kpi_card("💰 Costos", [("Recursos huérfanos", f"{cs['orphan_resources']} ({cs['pct_orphan_resources']}%)"),
                                      ("Instancias ociosas", f"{cs['pct_idle_instances']}%"),
                                      ("Desperdicio/mes", f"${cs['monthly_waste_usd']:,.2f}"),
                                      ("Servicios siempre encendidos", cs.get("always_on_services", 0)),
                                      ("Presupuesto configurado", {True: "Sí", False: "No"}.get(cs.get("has_budget"), "N/D")),
                                      ("Costo real 30 días", f"${cs['total_cost_30d_usd']:,.2f}" if cs.get("total_cost_30d_usd") is not None else "N/D")])

            if k.get("pipelines", {}).get("flows") or k.get("data_governance", {}).get("tables") or k.get("capacity", {}).get("clusters"):
                dg, pl, cp = k.get("data_governance", {}), k.get("pipelines", {}), k.get("capacity", {})
                d1, d2, d3 = st.columns(3)
                with d1:
                    kpi_card("🗄️ Gobierno de datos", [
                        ("Tablas / buckets", f"{dg.get('tables', 0)} / {dg.get('buckets', 0)}"),
                        ("Tablas con capa identificada", f"{dg.get('pct_tables_with_layer') or 0}%"),
                        ("Tablas grandes particionadas", f"{dg.get('pct_large_tables_partitioned') if dg.get('pct_large_tables_partitioned') is not None else '—'}%"),
                        ("Violaciones de capa", dg.get("layer_violations", 0)),
                        ("Problemas de normalización", dg.get("normalization_issues", 0)),
                        ("Problemas en el lake", dg.get("lake_issues", 0))])
                with d2:
                    kpi_card("🔄 Flujos", [
                        ("Flujos", pl.get("flows", 0)), ("Inactivos", f"{pl.get('pct_inactive', 0)}%"),
                        ("Con fallos", f"{pl.get('pct_failing', 0)}%"), ("Conflictos / ciclos", f"{pl.get('conflicts', 0)} / {pl.get('cycles', 0)}"),
                        ("Lentos / escaneos completos", f"{pl.get('slow_flows', 0)} / {pl.get('full_scans', 0)}"),
                        ("Costo flujos 30 d", f"${pl.get('flows_cost_30d_usd', 0):,.2f}")])
                with d3:
                    kpi_card("🖥️ Capacidad", [
                        ("Clusters", cp.get("clusters", 0)), ("Sobredimensionados", cp.get("oversized", 0)),
                        ("Subdimensionados", cp.get("undersized", 0)), ("Sin datos de uso", cp.get("no_data", 0)),
                        ("Costo actual/mes", f"${cp.get('current_monthly_usd', 0):,.2f}"),
                        ("Ahorro potencial/mes", f"${cp.get('potential_saving_usd', 0):,.2f}")])

            sev = k["findings_by_severity"]
            fig = go.Figure(go.Bar(x=list(sev), y=list(sev.values()), marker_color=[SEV_COLORS[s] for s in sev]))
            fig.update_layout(title="Hallazgos por severidad", height=300, margin=dict(l=20, r=20, t=50, b=20))
            st.plotly_chart(fig, width="stretch")

    with tab_report:
        report = result.get("final_report") or "_Sin informe._"
        render_markdown(report)
        st.download_button("⬇️ Descargar informe (.md)", report,
                           file_name=f"informe_deuda_{result['provider']}_{result['target_id']}.md", mime="text/markdown")

    with tab_findings:
        f = result.get("findings") or []
        if f:
            df = pd.DataFrame(f)[["severity", "category", "rule", "resource_id", "title", "recommendation",
                                  "estimated_monthly_waste_usd"]]
            cats = st.multiselect("Categoría", sorted(df.category.unique()), default=sorted(df.category.unique()))
            st.dataframe(df[df.category.isin(cats)], width="stretch", hide_index=True)
            st.download_button("⬇️ Exportar CSV", df.to_csv(index=False), "hallazgos.csv", "text/csv")
        else:
            st.info("No se registraron hallazgos.")

    with tab_raw:
        st.json(result.get("raw_infra_data") or {}, expanded=False)


# ---------------------------------------------------------------- Arquitectura
def show_identities(inv: dict[str, Any]) -> None:
    ids = [i for i in inv.get("service_identities", []) if not i.get("google_managed")]
    if not ids:
        st.info("No se detectaron cuentas de servicio (requiere Cloud Asset Inventory).")
        return
    st.caption("Cada cuenta de servicio, qué recursos la usan para ejecutarse o comunicarse y qué permisos tiene.")
    for i in sorted(ids, key=lambda x: (not x["overprivileged"], x["resource_id"])):
        flag = "🔴" if i["overprivileged"] else ("🟡" if i["is_default_sa"] else "🟢")
        with st.expander(f"{flag} {i['resource_id']}" + (" · (por defecto)" if i["is_default_sa"] else "")
                         + f" · usada por {len(i['used_by'])}"):
            a, b = st.columns(2)
            a.markdown("**Usada por**\n" + ("\n".join(f"- `{u}`" for u in i["used_by"]) or "_nadie (candidata a eliminar)_"))
            a.markdown("**Claves gestionadas por el usuario:** "
                       f"{i.get('user_managed_keys', 'N/D')} · antigüedad máx.: {i.get('oldest_key_age_days') or 'N/D'} días")
            b.markdown("**Roles en el proyecto**\n" + ("\n".join(
                f"- `{r}`" + (" ⚠️" if r in (i["broad_roles"] + i["privilege_escalation_roles"]) else "")
                for r in i["project_roles"]) or "_ninguno_"))
            if i["resource_grants"]:
                b.markdown("**Permisos sobre recursos específicos**\n" + "\n".join(
                    f"- `{g['role']}` en `{g['resource']}`" for g in i["resource_grants"]))


def show_billing(inv: dict[str, Any]) -> None:
    b = inv.get("billing")
    if not b:
        st.info("Sin datos de facturación.")
        return
    c = st.columns(3)
    c[0].metric("Facturación habilitada", {True: "Sí", False: "No"}.get(b.get("billing_enabled"), "N/D"))
    c[1].metric("Presupuesto para el proyecto", {True: "Sí", False: "No"}.get(b.get("has_budget"), "N/D"))
    c[2].metric("Costo neto 30 días", f"${b['total_cost_30d_usd']:,.2f}" if b.get("total_cost_30d_usd") is not None else "N/D")
    if b.get("billing_account"):
        st.caption(f"Cuenta de facturación: `{b['billing_account']}`")
    if b.get("budget_error"):
        st.caption(f"⚠️ No se pudieron leer presupuestos: {b['budget_error']}")
    for bud in b.get("budgets") or []:
        st.write(f"• **{bud['name']}**: {bud.get('amount')} {bud.get('currency') or ''} · alertas en {bud.get('thresholds')}")
    if b.get("cost_by_service_30d"):
        df = pd.DataFrame(b["cost_by_service_30d"])
        fig = go.Figure(go.Bar(x=df["net_cost"], y=df["service"], orientation="h", marker_color="#4285F4"))
        fig.update_layout(title="Costo neto por servicio (30 días)", height=max(260, 28 * len(df)),
                          yaxis={"autorange": "reversed"}, margin=dict(l=10, r=10, t=50, b=10))
        st.plotly_chart(fig, width="stretch")
    else:
        st.info("Para ver el costo real por servicio, indica en el panel lateral la tabla del "
                "export de facturación a BigQuery (Facturación → Exportación de facturación).")
    apis = inv.get("enabled_apis") or []
    if apis:
        with st.expander(f"APIs habilitadas en el proyecto ({len(apis)})"):
            st.write(", ".join(f"`{a}`" for a in apis))


# ---------------------------------------------------------------- Main
def main() -> None:
    st.title("☁️ Cloud Tech Debt Analyzer")
    st.caption("Auditoría multi-agente (LangGraph) de deuda técnica en Seguridad, Gobierno y Costos.")

    params = sidebar()
    if params:
        st.session_state.pop("result", None)
        backend_health.clear()
        job_id = start_audit(params["api_key"], params["body"])
        if job_id:
            st.session_state["job_id"] = job_id

    if st.session_state.get("job_id"):
        poll_job()
    elif "result" in st.session_state:
        st.divider()
        show_results(st.session_state["result"])
    elif not params:
        st.info("👈 Completa el panel lateral (API Key, proveedor, alcance e ID) y pulsa **Iniciar Auditoría**.")


main()
