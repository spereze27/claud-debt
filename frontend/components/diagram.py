"""Diagramas interactivos con íconos oficiales de GCP.

  · Arquitectura de servicios: quién se comunica con quién, a través de qué identidad y con qué permisos.
  · Linaje de datos por capas: fuentes → bronze → silver → gold → consumo, con los flujos entre medias.
"""
from __future__ import annotations

import json
import re
from typing import Any

import pandas as pd
import streamlit as st

from components.icons import FLOW_ICON, SYSTEM_ICON, icon_uri, icons_available

TYPE_COLORS = {
    "Cloud Run": "#4285F4", "Cloud Functions": "#1A73E8", "Compute Engine": "#5E97F6", "GKE": "#3367D6",
    "Service Account": "#F9AB00", "Pub/Sub Topic": "#34A853", "Pub/Sub Subscription": "#81C995",
    "Cloud Scheduler": "#A142F4", "Eventarc": "#C58AF9", "Cloud SQL": "#EA4335", "BigQuery": "#E8710A",
    "Cloud Storage": "#FA7B17", "Secret Manager": "#9AA0A6", "Internet": "#D93025",
}
EDGE_LABELS = {
    "runs_as": "se ejecuta como", "invokes": "invoca (HTTP)", "push": "push HTTP", "subscribes": "suscripción",
    "triggers": "dispara", "publishes": "publica", "connects_sql": "conecta a SQL", "reads_secret": "lee secreto",
    "uses_vpc": "usa VPC", "references": "referencia (inferido)", "has_role": "tiene rol",
    "public_access": "acceso público", "writes_to": "escribe en", "transport": "transporte",
}
# Columna (nivel) de cada tipo en el esquema de servicios, de izquierda a derecha.
SERVICE_LEVEL = {
    "Internet": 0, "Usuario/Grupo": 0, "Endpoint externo": 0, "Evento": 0,
    "Cloud Scheduler": 1, "Eventarc": 1, "Pub/Sub Topic": 1, "Pub/Sub Subscription": 2, "Cloud Tasks": 1,
    "Cloud Run": 3, "Cloud Run Job": 3, "Cloud Functions": 3, "Compute Engine": 3, "GKE": 3, "Workflows": 3,
    "App Engine": 3, "Composer": 3, "Dataflow": 3, "Dataproc": 3,
    "VPC Connector": 4, "Secret Manager": 4, "Cloud SQL": 5, "BigQuery": 5, "Cloud Storage": 5, "Firestore": 5,
    "Memorystore": 5, "Bigtable": 5, "Spanner": 5, "Service Account": 6,
}
LAYER_LEVEL = {"source": 0, "landing": 1, "bronze": 2, "silver": 4, "gold": 6}
MODE_LABEL = {"overwrite": "SOBRESCRIBE", "append": "append", "merge": "merge", "update": "update", "create": "crea"}
STATE_BORDER = {"active": "#34A853", "inactive": "#9AA0A6", "failing": "#D93025", "unknown": "#FBBC04"}


def _theme() -> tuple[str, str]:
    try:
        dark = (st.context.theme.type or "dark") == "dark"
    except Exception:  # noqa: BLE001
        dark = True
    return ("#0e1117", "#e8eaed") if dark else ("#ffffff", "#202124")


def _net(height: int, hierarchical: bool) -> Any:
    from pyvis.network import Network

    bg, fg = _theme()
    net = Network(height=f"{height}px", width="100%", directed=True, bgcolor=bg, font_color=fg)
    opts: dict[str, Any] = {
        "interaction": {"hover": True, "navigationButtons": True, "tooltipDelay": 80, "multiselect": True},
        "nodes": {"font": {"size": 12, "color": fg, "strokeWidth": 3, "strokeColor": bg},
                  "shapeProperties": {"useBorderWithImage": True, "interpolation": False}},
        "edges": {"arrows": {"to": {"enabled": True, "scaleFactor": 0.6}}, "font": {"size": 9, "color": fg,
                  "strokeWidth": 3, "strokeColor": bg, "align": "middle"}},
    }
    if hierarchical:
        opts["layout"] = {"hierarchical": {"enabled": True, "direction": "LR", "sortMethod": "directed",
                                           "levelSeparation": 230, "nodeSpacing": 95, "treeSpacing": 120,
                                           "blockShifting": True, "edgeMinimization": True, "parentCentralization": True}}
        opts["physics"] = {"enabled": False}
        opts["edges"]["smooth"] = {"type": "cubicBezier", "forceDirection": "horizontal", "roundness": 0.5}
    else:
        opts["physics"] = {"barnesHut": {"gravitationalConstant": -9000, "springLength": 170}, "stabilization": {"iterations": 250}}
    net.set_options(json.dumps(opts))
    return net


def _add_node(net: Any, nid: str, label: str, kind: str, title: str, level: int | None = None,
              border: str | None = None, size: int = 24) -> None:
    uri = icon_uri(kind)
    kw: dict[str, Any] = {"label": label[:38], "title": title}
    if level is not None:
        kw["level"] = level
    if uri:
        kw.update(shape="image", image=uri, size=size,
                  color={"border": border or "rgba(0,0,0,0)", "background": "rgba(0,0,0,0)"},
                  borderWidth=3 if border else 0)
    else:
        kw.update(shape="dot", size=size * 0.6, color=TYPE_COLORS.get(kind, "#9AA0A6"),
                  borderWidth=3 if border else 1)
        if border:
            kw["color"] = {"background": TYPE_COLORS.get(kind, "#9AA0A6"), "border": border}
    net.add_node(nid, **kw)


def embed_html(html: str, height: int) -> None:
    """Incrusta HTML con JavaScript (st.iframe en Streamlit reciente; components.html como respaldo)."""
    if hasattr(st, "iframe"):
        st.iframe(html, height=height)
    else:
        import streamlit.components.v1 as components

        components.html(html, height=height, scrolling=False)


def _render(net: Any, height: int) -> None:
    # pyvis serializa nodos/aristas con `tojson` (escapa <, >, &) y vis-network muestra los títulos como texto plano.
    embed_html(net.generate_html(), height + 20)


def _missing_libs() -> bool:
    try:
        import pyvis  # noqa: F401
    except ImportError:
        st.warning("Instala `pyvis` para ver los diagramas (`python -m pip install -r requirements.txt`).")
        return True
    if not icons_available():
        st.caption("ℹ️ Instala el paquete `diagrams` para ver los íconos oficiales de GCP en los diagramas.")
    return False


# ======================================================================= arquitectura de servicios
def services_diagram(inv: dict[str, Any]) -> None:
    arch = inv.get("architecture") or {}
    nodes, edges = arch.get("nodes", []), arch.get("edges", [])
    if not nodes:
        st.info("No hay datos de arquitectura. Requiere Cloud Asset Inventory (API cloudasset.googleapis.com y "
                "rol roles/cloudasset.viewer).")
        return
    if _missing_libs():
        return
    projects = sorted({n.get("project_id") for n in nodes if n.get("project_id")})
    c = st.columns([2, 1, 1, 1, 1])
    layout = c[0].radio("Distribución", ["Esquema por capas", "Libre"], horizontal=True, key="svc_layout")
    show_iam = c[1].toggle("Permisos IAM", value=False, key="svc_iam")
    show_sa = c[2].toggle("Cuentas de servicio", value=True, key="svc_sa")
    show_inf = c[3].toggle("Inferidas", value=True, key="svc_inf")
    only_con = c[4].toggle("Sólo conectados", value=True, key="svc_con")
    proj = st.selectbox("Proyecto", ["Todos"] + projects, key="svc_proj") if len(projects) > 1 else "Todos"

    types = {n["id"]: n["type"] for n in nodes}
    vis_edges = [e for e in edges
                 if (show_iam or e["kind"] != "has_role") and (show_inf or not e.get("inferred"))
                 and (show_sa or (types.get(e["source"]) != "Service Account" and types.get(e["target"]) != "Service Account"))]
    vis_nodes = [n for n in nodes if (proj == "Todos" or n.get("project_id") in (proj, None))
                 and (show_sa or n["type"] != "Service Account")]
    ids = {n["id"] for n in vis_nodes}
    vis_edges = [e for e in vis_edges if e["source"] in ids and e["target"] in ids]
    if only_con:
        connected = {e["source"] for e in vis_edges} | {e["target"] for e in vis_edges}
        vis_nodes = [n for n in vis_nodes if n["id"] in connected]
    if len(vis_nodes) > 250:
        st.caption(f"Mostrando 250 de {len(vis_nodes)} recursos; filtra por proyecto para ver el resto.")
        vis_nodes = vis_nodes[:250]
        ids = {n["id"] for n in vis_nodes}
        vis_edges = [e for e in vis_edges if e["source"] in ids and e["target"] in ids]

    hier = layout == "Esquema por capas"
    net = _net(680, hier)
    for n in vis_nodes:
        _add_node(net, n["id"], n["label"], n["type"], f"{n['type']}\n{n['label']}",
                  level=SERVICE_LEVEL.get(n["type"], 5) if hier else None)
    for e in vis_edges:
        tip = [EDGE_LABELS.get(e["kind"], e["kind"])]
        if e.get("via"):
            tip.append(f"identidad: {e['via']}")
        if e.get("permissions"):
            tip.append("permisos: " + ", ".join(e["permissions"][:8]))
        if e.get("detail"):
            tip.append(str(e["detail"]))
        danger = e["kind"] == "public_access" or str(e.get("detail") or "").startswith("SIN")
        net.add_edge(e["source"], e["target"], title="\n".join(tip),
                     label=(e.get("detail") or "")[:28] if e["kind"] == "has_role" else EDGE_LABELS.get(e["kind"], e["kind"]),
                     dashes=bool(e.get("inferred")) or e["kind"] in ("has_role", "runs_as"),
                     color="#D93025" if danger else ("#F9AB00" if e["kind"] == "runs_as" else "#8ab4f8"))
    if hier:
        st.caption("Entrada → Orquestación/mensajería → Cómputo → Secretos/red → Datos → Identidades")
    _render(net, 680)
    st.caption("Pasa el cursor por una flecha para ver la identidad y sus permisos. Naranja = identidad de ejecución; "
               "rojo = acceso público o sin autenticación; discontinua = permiso IAM o relación inferida.")

    labels = {n["id"]: f"{n['label']} ({n['type']})" for n in nodes}
    rows = [{"Origen": labels.get(e["source"], e["source"]), "Mecanismo": EDGE_LABELS.get(e["kind"], e["kind"]),
             "Destino": labels.get(e["target"], e["target"]), "Identidad": e.get("via") or "",
             "Permisos de la identidad": ", ".join(e.get("permissions") or []), "Detalle": e.get("detail") or "",
             "Inferido": "sí" if e.get("inferred") else ""} for e in vis_edges]
    with st.expander(f"Flujos de comunicación ({len(rows)})"):
        st.dataframe(pd.DataFrame(rows), width="stretch", hide_index=True)


# ======================================================================= linaje de datos por capas
def _lineage_levels(flows: list[dict[str, Any]], data_nodes: dict[str, dict[str, Any]]) -> tuple[dict, dict]:
    dlevel = {nid: LAYER_LEVEL.get(n.get("layer") or "", None) for nid, n in data_nodes.items()}
    flevel: dict[str, int] = {}
    for _ in range(3):  # propagación simple para datos sin capa
        for f in flows:
            ins = [dlevel.get(i["id"]) for i in f["inputs"]]
            ins = [x for x in ins if x is not None]
            flevel[f["resource_id"]] = (max(ins) + 1) if ins else 1
            for o in f["outputs"]:
                if dlevel.get(o["id"]) is None:
                    dlevel[o["id"]] = flevel[f["resource_id"]] + 1
        for f in flows:
            for i in f["inputs"]:
                if dlevel.get(i["id"]) is None:
                    dlevel[i["id"]] = max(0, flevel[f["resource_id"]] - 1)
    return flevel, {k: (v if v is not None else 3) for k, v in dlevel.items()}


def lineage_diagram(dp: dict[str, Any], key: str = "lin") -> None:
    flows_all = dp.get("flows") or []
    if not flows_all:
        st.info("No se detectaron flujos de datos (BigQuery, Dataflow, Dataproc, Composer, Datastream, microservicios).")
        return
    if _missing_libs():
        return
    data_nodes = {n["id"]: n for n in (dp.get("lineage") or {}).get("nodes", [])}
    projects = sorted({f["project_id"] for f in flows_all})
    c = st.columns([2, 2, 1, 1])
    proj = c[0].selectbox("Proyecto", ["Todos"] + projects, key=f"{key}_p") if len(projects) > 1 else "Todos"
    kinds = sorted({f["engine"] for f in flows_all})
    sel_kinds = c[1].multiselect("Motores", kinds, default=kinds, key=f"{key}_k")
    hide_inactive = c[2].toggle("Ocultar inactivos", value=False, key=f"{key}_i")
    only_issues = c[3].toggle("Sólo con problemas", value=False, key=f"{key}_o")
    flows = [f for f in flows_all if (proj == "Todos" or f["project_id"] == proj) and f["engine"] in sel_kinds
             and not (hide_inactive and f["state"] == "inactive") and not (only_issues and not f.get("signals"))]
    flows = sorted(flows, key=lambda f: (-len(f.get("signals") or []), -f["cost_30d_usd"]))[:120]
    if not flows:
        st.info("Ningún flujo coincide con los filtros.")
        return
    flevel, dlevel = _lineage_levels(flows, data_nodes)
    net = _net(720, True)
    used: set[str] = set()
    for f in flows:
        for io in f["inputs"] + f["outputs"]:
            used.add(io["id"])
    for nid in used:
        n = data_nodes.get(nid, {"name": nid, "system": None})
        kind = SYSTEM_ICON.get(n.get("system") or "", "ext")
        layer = n.get("layer")
        _add_node(net, nid, (f"[{layer}] " if layer and layer != "source" else "") + str(n.get("name", nid)).split("/")[-1][:34],
                  kind, f"{n.get('system')}: {n.get('name')}\ncapa: {layer or '—'} ({n.get('layer_source', '')})"
                        + (f"\nformato: {n.get('format')}" if n.get("format") else ""), level=dlevel.get(nid, 3), size=20)
    for f in flows:
        sig = "\n".join(f"⚠ {s['text'][:140]}" for s in (f.get("signals") or [])[:4])
        title = (f"{f['engine']}\n{f['name']}\nestado: {f['state_es']} · {f.get('schedule') or 's/calendario'}\n"
                 f"media {f.get('avg_duration_s') or '—'} s · p95 {f.get('p95_duration_s') or '—'} s\n"
                 f"costo 30 d: ${f['cost_30d_usd']:,.2f}" + (f"\n{sig}" if sig else ""))
        _add_node(net, f["resource_id"], f["name"], FLOW_ICON.get(f["kind"], "ext"), title,
                  level=flevel[f["resource_id"]], border=STATE_BORDER.get(f["state"]), size=28)
        for i in f["inputs"]:
            net.add_edge(i["id"], f["resource_id"], dashes=True, color="#8ab4f8", title="lee")
        for o in f["outputs"]:
            mode = o.get("write_mode") or ""
            bad = "overwrite" in mode and (data_nodes.get(o["id"], {}).get("layer") == "bronze")
            back = dlevel.get(o["id"], 9) < flevel[f["resource_id"]]
            net.add_edge(f["resource_id"], o["id"], label=MODE_LABEL.get(mode, mode),
                         color="#D93025" if bad or back else ("#F9AB00" if "overwrite" in mode else "#34A853"),
                         title=f"escribe ({mode or 'modo desconocido'})" + (" · ¡flujo hacia una capa inferior!" if back else ""))
    st.markdown(
        "<div style='display:flex;justify-content:space-between;font-size:.8rem;opacity:.8;padding:0 6px'>"
        + "".join(f"<span>{x}</span>" for x in ("Fuentes", "Ingesta", "🥉 Bronze", "Transformación", "🥈 Silver",
                                                  "Transformación", "🥇 Gold", "Consumo")) + "</div>",
        unsafe_allow_html=True)
    _render(net, 720)
    st.caption("Borde del flujo: verde activo · gris inactivo · rojo con fallos. Flechas de escritura: verde append/merge, "
               "naranja sobrescribe, rojo sobrescritura de bronze o dato que vuelve a una capa inferior.")


# ======================================================================= Markdown con Mermaid
def render_markdown(text: str) -> None:
    """st.markdown no dibuja Mermaid: se renderiza cada bloque ```mermaid con mermaid.js.

    El código lo escribe el LLM a partir de nombres de recursos (no confiables): nunca se inserta como HTML;
    se pasa como cadena JSON a mermaid.render con securityLevel 'strict' (saneado con DOMPurify).
    """
    parts = re.split(r"```mermaid\s*\n(.*?)```", text or "", flags=re.S)
    bg, fg = _theme()
    for idx, part in enumerate(parts):
        if idx % 2 == 0:
            if part.strip():
                st.markdown(part)
        else:
            height = min(900, 160 + 38 * part.count("\n"))
            code = json.dumps(part).replace("</", "<\\/")
            theme = "dark" if bg != "#ffffff" else "default"
            embed_html(
                f"""<div id="m" style="background:{bg};overflow:auto"></div>
                <script type="module">
                import mermaid from 'https://cdn.jsdelivr.net/npm/mermaid@11/dist/mermaid.esm.min.mjs';
                mermaid.initialize({{startOnLoad: false, securityLevel: 'strict', theme: '{theme}'}});
                try {{ const {{svg}} = await mermaid.render('g{idx}', {code});
                      document.getElementById('m').innerHTML = svg; }}
                catch (e) {{ document.getElementById('m').textContent = 'No se pudo dibujar el diagrama: ' + e.message; }}
                </script>""", height)
            with st.expander("Código del diagrama"):
                st.code(part, language="text")
