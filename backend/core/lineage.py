"""Linaje de flujos encadenados: entradas/salidas crudas, capas inferidas y grupos de cascada.

Funciones PURAS (sin red) que operan sobre los flujos ya construidos por `data_platform`:

  · `safe_*`               normalización de campos que GCP puede devolver nulos, vacíos o con formas distintas
  · `parse_label_string`   "k1:v1,k2:v2" (@@query_label) → dict
  · `infer_layer_from`     capa por etiquetas (capa/layer/…) → regex sobre nombre/ruta → "no_detectada"
  · `flow_dependencies`    flujo A → flujo B cuando una salida de A es entrada de B
  · `assign_cascades`      grupo_cascada_id por (1) etiquetas del flujo/job, (2) convención "[grupo] nombre",
                           (3) etiquetas de los recursos que escribe/lee, (4) herencia por linaje y
                           (5) componentes conexos ("auto-…"); más el orden de cada flujo dentro de su cascada
  · `lineage_issues`       flujos sin grupo declarado, cascadas con varios dueños, flujos sin linaje detectable
"""
from __future__ import annotations

import hashlib
import re
from collections import Counter, defaultdict, deque
from typing import Any, Iterable

NOT_DETECTED = "no_detectada"
GROUP_KEYS = ("grupo_cascada", "grupo-cascada", "grupo", "cascade", "cascade_group", "cascada", "pipeline_group",
              "flow_group", "group")
OWNER_KEYS = ("owner", "dueno", "propietario", "team", "equipo")
LAYER_KEYS = ("capa", "layer", "medallion", "data_layer", "data-layer", "zone", "zona")
FLOW_LAYER_IN_KEYS = ("capa_entrada", "layer_in", "input_layer")
FLOW_LAYER_OUT_KEYS = ("capa_salida", "layer_out", "output_layer")
# "[ingesta-core] f1_…", "ingesta-core__f1_…", "ingesta-core :: f1"
NAME_GROUP_RE = re.compile(r"^\s*\[([A-Za-z0-9_\-]{2,63})\]|^([a-z0-9][a-z0-9\-]{1,62})(?:__|\s*::\s*)", re.I)
LABEL_VALUE_RE = re.compile(r"^[a-z0-9_\-]{1,63}$")


# ============================================================ nulos y formas variables
def safe_str(v: Any) -> str | None:
    """None, '', espacios, 'null'/'None' → None; el resto como texto sin espacios laterales."""
    if v is None:
        return None
    s = str(v).strip()
    return s if s and s.lower() not in ("null", "none", "nan") else None


def safe_list(v: Any) -> list[Any]:
    if v is None:
        return []
    if isinstance(v, (list, tuple, set)):
        return [x for x in v if x is not None and safe_str(x) is not None]
    return [v] if safe_str(v) else []


def safe_labels(v: Any) -> dict[str, str]:
    """Etiquetas en cualquiera de las formas que devuelve GCP → dict[str, str] en minúsculas.

    · dict {"k": "v"}                              (API de recursos)
    · lista [{"key": "k", "value": "v"}]           (INFORMATION_SCHEMA.JOBS.labels)
    · lista [("k", "v")]                           (OPTIONS(labels=…) ya parseado)
    · str 'k1:v1,k2:v2'                            (@@query_label)
    """
    out: dict[str, str] = {}
    if v is None:
        return out
    if isinstance(v, str):
        return parse_label_string(v)
    if isinstance(v, dict):
        items: Iterable[tuple[Any, Any]] = v.items()
    else:
        items = []
        for x in v:
            if isinstance(x, dict):
                items.append((x.get("key"), x.get("value")))  # type: ignore[union-attr]
            elif isinstance(x, (list, tuple)) and len(x) == 2:
                items.append((x[0], x[1]))  # type: ignore[union-attr]
    for k, val in items:
        k2, v2 = safe_str(k), safe_str(val)
        if k2:
            out[k2.lower()] = (v2 or "").lower()
    return out


def parse_label_string(s: str | None) -> dict[str, str]:
    """'grupo_cascada:ingesta-core,owner:finanzas' → {'grupo_cascada': 'ingesta-core', 'owner': 'finanzas'}."""
    out: dict[str, str] = {}
    for part in (s or "").split(","):
        k, sep, v = part.partition(":")
        k, v = k.strip().strip("'\"").lower(), v.strip().strip("'\"").lower()
        if sep and k:
            out[k] = v
    return out


def query_label_from_sql(sql: str | None) -> dict[str, str]:
    """Etiquetas declaradas en un script con SET @@query_label = "…" (puede haber varias; gana la última)."""
    out: dict[str, str] = {}
    for m in re.finditer(r"SET\s+@@query_label\s*=\s*(['\"])(.*?)\1", sql or "", re.I | re.S):
        out.update(parse_label_string(m.group(2)))
    return out


def first_label(labels: dict[str, str] | None, keys: Iterable[str]) -> str | None:
    for k in keys:
        v = safe_str((labels or {}).get(k))
        if v:
            return v
    return None


def group_from_name(name: str | None) -> str | None:
    m = NAME_GROUP_RE.match(name or "")
    if not m:
        return None
    g = (m.group(1) or m.group(2) or "").lower()
    return g if LABEL_VALUE_RE.match(g) else None


# ============================================================ capas
def infer_layer_from(name: str | None, container: str | None = None, labels: dict[str, str] | None = None,
                     system: str | None = None) -> tuple[str, str]:
    """(capa, cómo_se_infirió). Nunca lanza excepción: ante datos nulos devuelve (no_detectada, motivo)."""
    from backend.core.data_platform import classify_layer

    lab = first_label(labels, LAYER_KEYS)
    if lab:
        layer, src = classify_layer(None, None, {"layer": lab})
        if layer:
            return layer, f"etiqueta capa={lab}"
    if system in ("postgres", "mysql", "oracle", "sqlserver", "hdfs", "kafka", "http", "sftp", "ext"):
        return "source", f"sistema origen ({system})"
    layer, src = classify_layer(safe_str(name), safe_str(container), None)
    return (layer, src) if layer else (NOT_DETECTED, "sin evidencia en etiquetas ni nombres")


def summarize_layers(layers: Iterable[str | None]) -> str:
    """Capa representativa de un conjunto de entradas/salidas (la más 'alta'); no_detectada si ninguna."""
    from backend.core.data_platform import LAYER_ORDER

    known = [x for x in layers if x and x in LAYER_ORDER]
    return max(known, key=lambda x: LAYER_ORDER[x]) if known else NOT_DETECTED


# ============================================================ dependencias entre flujos
def flow_dependencies(flows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Arista origen→destino por cada dato que un flujo escribe y otro lee. Ignora auto-dependencias."""
    readers: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for f in flows:
        for i in f.get("inputs") or []:
            if i.get("id"):
                readers[i["id"]].append(f)
    out, seen = [], set()
    for f in flows:
        for o in f.get("outputs") or []:
            for g in readers.get(o.get("id") or "", []):
                key = (f["resource_id"], g["resource_id"], o["id"])
                if g["resource_id"] == f["resource_id"] or key in seen:
                    continue
                seen.add(key)
                out.append({"origen_id": f["resource_id"], "origen_nombre": f.get("name"),
                            "destino_id": g["resource_id"], "destino_nombre": g.get("name"),
                            "dato_id": o["id"], "dato": o.get("name") or o["id"],
                            "capa_dato": o.get("layer") or NOT_DETECTED, "modo_escritura_origen": o.get("write_mode")})
    return out


def _components(nodes: list[str], edges: list[tuple[str, str]]) -> list[list[str]]:
    adj: dict[str, set[str]] = defaultdict(set)
    for a, b in edges:
        adj[a].add(b)
        adj[b].add(a)
    seen: set[str] = set()
    comps = []
    for n in nodes:
        if n in seen:
            continue
        comp, q = [], deque([n])
        seen.add(n)
        while q:
            u = q.popleft()
            comp.append(u)
            for v in adj[u]:
                if v not in seen:
                    seen.add(v)
                    q.append(v)
        comps.append(sorted(comp))
    return comps


def _depth(members: set[str], edges: list[tuple[str, str]]) -> dict[str, int]:
    """Orden dentro de la cascada (1 = primer eslabón). Tolera ciclos: los nodos del ciclo comparten nivel."""
    preds: dict[str, set[str]] = defaultdict(set)
    for a, b in edges:
        if a in members and b in members:
            preds[b].add(a)
    depth: dict[str, int] = {}

    def visit(n: str, stack: frozenset[str]) -> int:
        if n in depth:
            return depth[n]
        ps = [p for p in preds[n] if p not in stack]
        d = 1 + max((visit(p, stack | {n}) for p in ps), default=0)
        depth[n] = d
        return d

    for m in sorted(members):
        visit(m, frozenset())
    return depth


# ============================================================ grupos de cascada
def _resource_labels_for(ios: list[dict[str, Any]], node_labels: dict[str, dict[str, str]]) -> list[dict[str, str]]:
    return [node_labels.get(i.get("id") or "", {}) for i in ios]


def assign_cascades(flows: list[dict[str, Any]], node_labels: dict[str, dict[str, str]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Enriquece cada flujo con grupo_cascada_id, origen_grupo, owner y orden_en_cascada. Devuelve (dependencias, grupos)."""
    deps = flow_dependencies(flows)
    edges = [(d["origen_id"], d["destino_id"]) for d in deps]
    by_id = {f["resource_id"]: f for f in flows}

    # 1-3: evidencia propia del flujo
    for f in flows:
        labels = safe_labels(f.get("labels"))
        out_labels = _resource_labels_for(f.get("outputs") or [], node_labels)
        in_labels = _resource_labels_for(f.get("inputs") or [], node_labels)
        group, source = first_label(labels, GROUP_KEYS), "etiqueta del flujo/job"
        if not group:
            group, source = group_from_name(f.get("display_name") or f.get("name")), "convención de nombre [grupo]"
        if not group:
            votes = Counter(g for g in (first_label(lab, GROUP_KEYS) for lab in out_labels) if g)
            group, source = (votes.most_common(1)[0][0], "etiquetas de los recursos de salida") if votes else (None, None)
        if not group:
            votes = Counter(g for g in (first_label(lab, GROUP_KEYS) for lab in in_labels) if g)
            group, source = (votes.most_common(1)[0][0], "etiquetas de los recursos de entrada") if votes else (None, None)
        f["grupo_cascada_id"], f["origen_grupo"] = group, source if group else None
        owner = first_label(labels, OWNER_KEYS) or safe_str(f.get("owner"))
        if not owner:
            votes = Counter(o for o in (first_label(lab, OWNER_KEYS) for lab in out_labels + in_labels) if o)
            owner = votes.most_common(1)[0][0] if votes else None
        f["owner"] = owner

    # 4-5: herencia por linaje y componentes conexos
    for comp in _components(list(by_id), edges):
        declared = Counter(by_id[n]["grupo_cascada_id"] for n in comp if by_id[n]["grupo_cascada_id"])
        if len(declared) == 1:
            g = next(iter(declared))
            for n in comp:
                if not by_id[n]["grupo_cascada_id"]:
                    by_id[n]["grupo_cascada_id"], by_id[n]["origen_grupo"] = g, "heredado por linaje"
        elif not declared:
            auto = "auto-" + hashlib.sha1("|".join(comp).encode()).hexdigest()[:8]
            for n in comp:
                by_id[n]["grupo_cascada_id"], by_id[n]["origen_grupo"] = auto, (
                    "inferido por linaje (sin etiquetas)" if len(comp) > 1 else "flujo aislado (sin etiquetas)")
        else:  # varios grupos declarados conectados: los sin grupo quedan en un grupo propio del componente
            auto = "mixto-" + hashlib.sha1("|".join(comp).encode()).hexdigest()[:8]
            for n in comp:
                if not by_id[n]["grupo_cascada_id"]:
                    by_id[n]["grupo_cascada_id"], by_id[n]["origen_grupo"] = auto, "linaje conectado a varios grupos"

    # orden dentro de cada grupo + resumen
    members: dict[str, set[str]] = defaultdict(set)
    for f in flows:
        members[f["grupo_cascada_id"]].add(f["resource_id"])
    groups = []
    for gid, ms in sorted(members.items()):
        depth = _depth(ms, edges)
        for n in ms:
            by_id[n]["orden_en_cascada"] = depth.get(n, 1)
        ordered = sorted(ms, key=lambda n: (depth.get(n, 1), by_id[n].get("name") or ""))
        layers = []
        for n in ordered:
            for lay in (by_id[n].get("capa_entrada_inferida"), by_id[n].get("capa_salida_inferida")):
                if lay and lay != NOT_DETECTED and lay not in layers:
                    layers.append(lay)
        owners = sorted({by_id[n]["owner"] for n in ms if by_id[n].get("owner")})
        external = [d for d in deps if (d["origen_id"] in ms) != (d["destino_id"] in ms)]
        groups.append({
            "grupo_cascada_id": gid,
            "origen_inferencia": Counter(by_id[n]["origen_grupo"] for n in ms).most_common(1)[0][0],
            "num_flujos": len(ms), "profundidad": max(depth.values(), default=1),
            "es_cascada": len(ms) > 1 and any(a in ms and b in ms for a, b in edges),
            "flujos_en_orden": " → ".join(by_id[n].get("name") or n for n in ordered),
            "flujo_ids": ordered, "capas_recorridas": " → ".join(layers) or NOT_DETECTED,
            "owners": ", ".join(owners) or NOT_DETECTED, "proyectos": ", ".join(sorted({by_id[n].get("project_id") or "" for n in ms} - {""})),
            "costo_30d_usd": round(sum(float(by_id[n].get("cost_30d_usd") or 0) for n in ms), 2),
            "estados": dict(Counter(by_id[n].get("state") or "unknown" for n in ms)),
            "dependencias_con_otros_grupos": len(external),
        })
    for d in deps:
        d["grupo_origen"] = by_id[d["origen_id"]]["grupo_cascada_id"]
        d["grupo_destino"] = by_id[d["destino_id"]]["grupo_cascada_id"]
        d["mismo_grupo"] = d["grupo_origen"] == d["grupo_destino"]
    return deps, groups


def enrich_flow_lineage(f: dict[str, Any], nodes: dict[str, dict[str, Any]]) -> None:
    """fuente_raw / destino_raw y capas de entrada/salida inferidas (con prioridad a etiquetas del propio flujo)."""
    def raw_list(items: list[dict[str, Any]]) -> list[str]:
        vals = [safe_str(i.get("raw")) or safe_str(i.get("name")) or safe_str(i.get("id")) for i in items]
        return list(dict.fromkeys(v for v in vals if v))

    ins, outs = f.get("inputs") or [], f.get("outputs") or []
    f["fuente_raw"] = "; ".join(raw_list(ins)) or NOT_DETECTED
    f["destino_raw"] = "; ".join(raw_list(outs)) or NOT_DETECTED
    labels = safe_labels(f.get("labels"))
    lin, lout = first_label(labels, FLOW_LAYER_IN_KEYS), first_label(labels, FLOW_LAYER_OUT_KEYS)
    for side, items, declared in (("entrada", ins, lin), ("salida", outs, lout)):
        if declared:
            layer, src = infer_layer_from(None, None, {"capa": declared})
            src = f"etiqueta del flujo {('capa_' + side)}={declared}"
        else:
            layer, src = summarize_layers(i.get("layer") for i in items), f"capa de los datos de {side}"
            if layer == NOT_DETECTED:
                src = "sin datos de " + side + " detectables" if not items else "datos sin capa identificable"
        f[f"capa_{side}_inferida"] = layer
        f[f"origen_capa_{side}"] = src


def lineage_issues(flows: list[dict[str, Any]], groups: list[dict[str, Any]]) -> list[dict[str, Any]]:
    issues = []
    for f in flows:
        if f.get("kind") in ("microservice",) and not (f.get("inputs") or f.get("outputs")):
            continue
        if not f.get("inputs") and not f.get("outputs"):
            issues.append({"code": "LINEAGE_UNDETECTED", "severity": "LOW", "resource_id": f["resource_id"],
                           "flows": [f["resource_id"]],
                           "text": f"{f['name']}: no se pudo detectar ninguna entrada ni salida (GCP no expone el linaje "
                                   "de este motor o el job no tiene destino). Documéntalo en el catálogo de flujos."})
        if (f.get("origen_grupo") or "").startswith(("inferido", "flujo aislado", "heredado", "linaje conectado")):
            issues.append({"code": "FLOW_UNGROUPED", "severity": "LOW", "resource_id": f["resource_id"],
                           "flows": [f["resource_id"]],
                           "text": f"{f['name']} no declara su grupo de flujos; se asignó a '{f['grupo_cascada_id']}' "
                                   f"({f['origen_grupo']}). Añade la etiqueta grupo_cascada al job o al recurso."})
    for g in groups:
        if "," in (g.get("owners") or "") and g["num_flujos"] > 1:
            issues.append({"code": "CASCADE_MULTI_OWNER", "severity": "MEDIUM", "resource_id": g["flujo_ids"][0],
                           "flows": g["flujo_ids"],
                           "text": f"La cascada '{g['grupo_cascada_id']}' ({g['flujos_en_orden']}) tiene varios dueños "
                                   f"({g['owners']}): no hay un responsable único de punta a punta."})
    return issues
