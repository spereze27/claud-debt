"""Motor determinista de la plataforma de datos (sin LLM).

Recibe lo recolectado de BigQuery, Data Transfer, Dataflow, Dataproc, Composer,
Datastream, Cloud Storage y los microservicios, y produce:

  · tables    evaluación por tabla: capa medallion, normalización, tipos, particionado, PII…
  · datasets  modelo de facturación de almacenamiento recomendado
  · lake      evaluación del data lake en GCS: formatos, archivos pequeños, clase de almacenamiento…
  · flows     cada flujo (pipeline/microservicio) con entradas, salidas, modo de escritura
              (append/overwrite/merge), calendario, estado, duración y costo estimado
  · lineage   grafo dato → flujo → dato
  · issues    conflictos, ciclos, inactividad, violaciones de capa, escaneos completos…
  · cost_breakdown  costos por proyecto → flujo (+ costos compartidos)

Todo lo que el LLM dice después se apoya en estas cifras.
"""
from __future__ import annotations

import hashlib
import re
import statistics
from collections import Counter, defaultdict
from datetime import datetime, timezone
from typing import Any, Iterable

from backend.core import pricing as P
from backend.core.lineage import (LAYER_KEYS, NOT_DETECTED, assign_cascades, enrich_flow_lineage, first_label,
                                  infer_layer_from, lineage_issues, query_label_from_sql, safe_labels, safe_list,
                                  safe_str)

# ============================================================ capas medallion
LAYER_ORDER = {"source": 0, "landing": 1, "bronze": 2, "silver": 3, "gold": 4}
LAYER_TOKENS = {
    "landing": {"landing", "land", "lz", "inbound", "dropzone", "aterrizaje", "entrada"},
    "bronze": {"bronze", "bronce", "raw", "ingest", "ingestion", "ingesta", "l0", "crudo",
               "cruda", "source", "src", "rawzone", "stage0", "inbound"},
    "silver": {"silver", "plata", "staging", "stg", "clean", "cleansed", "refined", "conformed", "trusted",
               "intermediate", "int", "l1", "processed", "curation", "limpio", "limpia", "ods"},
    "gold": {"gold", "oro", "curated", "mart", "marts", "dm", "datamart", "presentation", "analytics", "reporting",
             "report", "reports", "dwh", "agg", "aggregated", "l2", "serving", "bi", "kpi", "kpis", "semantic",
             "consumption", "consumo"},
}
LAYER_LABEL_KEYS = ("layer", "capa", "medallion", "data_layer", "data-layer", "tier_datos", "zone")
ANALYTIC_FORMATS = {"parquet", "avro", "orc", "delta", "iceberg"}
TEXT_FORMATS = {"csv", "json", "jsonl", "txt", "tsv", "xml", "xlsx", "xls", "dat", "ndjson"}

PII_RE = re.compile(r"(e?mail|correo|phone|tel[eé]fono|celular|movil|m[oó]vil|c[eé]dula|dni|ssn|nit|rut|curp|"
                    r"passport|pasaporte|tarjeta|card_?num|iban|cuenta_banc|address|direcci[oó]n|birth|nacimiento|"
                    r"salary|salario|ip_address|geoloc|latitud|longitud)", re.I)
DATE_NAME_RE = re.compile(r"(date|fecha|_at$|_dt$|timestamp|_ts$|^ts_|time$|hora|periodo|period)", re.I)
NUMERIC_NAME_RE = re.compile(r"(amount|monto|valor|price|precio|qty|cantidad|total|importe|cost|costo|saldo|"
                             r"balance|tasa|rate|percent|porcentaje)", re.I)
DESC_SUFFIX = ("name", "nombre", "desc", "descripcion", "description", "label", "etiqueta", "title", "titulo")
SQL_WRITE_RE = re.compile(r"\b(INSERT\s+(?:INTO\s+)?|MERGE\s+(?:INTO\s+)?|CREATE\s+(?:OR\s+REPLACE\s+)?(?:TEMP\s+)?TABLE\s+"
                          r"(?:IF\s+NOT\s+EXISTS\s+)?|TRUNCATE\s+TABLE\s+|UPDATE\s+|DELETE\s+(?:FROM\s+)?)"
                          r"(`?[\w\-]+[.:][\w]+\.[\w$]+`?|`?[\w]+\.[\w$]+`?)", re.I)
SQL_READ_RE = re.compile(r"\b(?:FROM|JOIN)\s+(`?[\w\-]+[.:][\w]+\.[\w$*]+`?|`?[\w]+\.[\w$*]+`?)", re.I)
SQL_OVERWRITE_RE = re.compile(r"\bCREATE\s+OR\s+REPLACE\s+TABLE|\bTRUNCATE\s+TABLE", re.I)


def _tokens(*parts: str | None) -> list[str]:
    toks: list[str] = []
    for p in parts:
        toks += [t for t in re.split(r"[^a-z0-9]+", (p or "").lower()) if t]
    return toks


def classify_layer(name: str | None = None, container: str | None = None,
                   labels: dict[str, str] | None = None) -> tuple[str | None, str]:
    """Devuelve (capa, origen_de_la_clasificación). Prioridad: etiqueta > nombre > contenedor."""
    for k, v in (labels or {}).items():
        if k.lower() in LAYER_LABEL_KEYS:
            for layer, toks in LAYER_TOKENS.items():
                if v.lower() in toks or v.lower() == layer:
                    return layer, f"etiqueta {k}={v}"
    for source, text in (("nombre", name), ("contenedor", container)):
        hits = [layer for layer, toks in LAYER_TOKENS.items() for t in _tokens(text) if t in toks]
        if hits:
            # el token más "alto" gana si hay varios (p.ej. stg_gold → gold)
            return max(hits, key=lambda layer: LAYER_ORDER[layer]), f"{source} '{text}'"
    return None, "sin evidencia"


# ============================================================ referencias a datos
def fmt_from_path(path: str) -> str | None:
    m = re.search(r"\.([a-z0-9]{2,8})(?:\.(?:gz|snappy|zst|bz2|zip))?$", path.lower())
    return m.group(1) if m else None


def _gcs_logical_prefix(path: str) -> str:
    """gs://b/raw/ventas/dt=2024-01-01/part-1.parquet → b/raw/ventas"""
    rest = path[5:] if path.startswith("gs://") else path
    parts = rest.split("/")
    keep = [parts[0]]
    for seg in parts[1:]:
        if not seg or "=" in seg or "*" in seg or "{" in seg or re.fullmatch(r"\d{1,8}", seg) or "." in seg:
            break
        keep.append(seg)
        if len(keep) >= 3:
            break
    return "/".join(keep)


def normalize_ref(ref: str, default_project: str | None = None) -> dict[str, Any] | None:
    """Convierte cualquier referencia a dato (URI, tabla, topic, JDBC…) en un nodo de datos normalizado."""
    r = (ref or "").strip().strip("`'\"")
    if not r or len(r) < 3:
        return None
    low = r.lower()
    if low.startswith("gs://"):
        prefix = _gcs_logical_prefix(r)
        return {"id": f"data:gcs:{prefix}", "system": "gcs", "name": f"gs://{prefix}", "format": fmt_from_path(r),
                "bucket": prefix.split("/")[0]}
    if low.startswith(("hdfs://", "/user/", "/data/", "/warehouse/")):
        path = re.sub(r"^hdfs://[^/]+", "", r)
        segs = [s for s in path.split("/") if s and "=" not in s and "*" not in s][:4]
        return {"id": f"data:hdfs:/{'/'.join(segs)}", "system": "hdfs", "name": f"hdfs:/{'/'.join(segs)}",
                "format": fmt_from_path(r)}
    m = re.match(r"^(?:jdbc:)?(postgres(?:ql)?|mysql|sqlserver|oracle(?::thin)?|mariadb)[:/@]+([^/:;?]+)(?::\d+)?[/:;]?(?:databaseName=)?([\w\-]*)", low)
    if m:
        engine = {"postgres": "postgres", "postgresql": "postgres"}.get(m.group(1), m.group(1).split(":")[0])
        return {"id": f"data:{engine}:{m.group(2)}/{m.group(3)}", "system": engine,
                "name": f"{engine}://{m.group(2)}/{m.group(3)}"}
    m = re.match(r"^projects/([^/]+)/(topics|subscriptions)/([^/]+)$", r)
    if m:
        kind = "pubsub" if m.group(2) == "topics" else "pubsub_sub"
        return {"id": f"data:{kind}:{m.group(3)}", "system": "pubsub", "name": m.group(3)}
    if low.startswith(("kafka://", "kafka:")):
        return {"id": f"data:kafka:{r.split('//')[-1]}", "system": "kafka", "name": r}
    if low.startswith(("http://", "https://")):
        return {"id": f"data:http:{r.split('?')[0]}", "system": "http", "name": r.split("?")[0]}
    m = re.match(r"^([a-z][a-z0-9\-]{4,29})[.:]([A-Za-z0-9_]+)\.([A-Za-z0-9_$*]+)$", r)
    if m:
        table = re.sub(r"\$.*$", "", m.group(3))
        return {"id": f"data:bq:{m.group(1)}.{m.group(2)}.{table}", "system": "bigquery",
                "name": f"{m.group(1)}.{m.group(2)}.{table}", "project": m.group(1), "dataset": m.group(2), "table": table}
    m = re.match(r"^([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z0-9_$*]+)$", r)
    if m and default_project and not m.group(1).lower() in ("information_schema",):
        table = re.sub(r"\$.*$", "", m.group(2))
        return {"id": f"data:bq:{default_project}.{m.group(1)}.{table}", "system": "bigquery",
                "name": f"{default_project}.{m.group(1)}.{table}", "project": default_project,
                "dataset": m.group(1), "table": table}
    return None


def sql_io(sql: str, default_project: str | None) -> tuple[list[str], list[tuple[str, str]]]:
    """Extrae (lecturas, [(escritura, modo)]) de un texto SQL."""
    if not sql:
        return [], []
    clean = re.sub(r"--[^\n]*|/\*.*?\*/", " ", sql, flags=re.S)
    writes = []
    for verb, target in SQL_WRITE_RE.findall(clean):
        v = verb.strip().upper()
        mode = ("merge" if v.startswith("MERGE") else "append" if v.startswith("INSERT")
                else "overwrite" if ("REPLACE" in v or v.startswith("TRUNCATE")) else "update" if v.startswith(("UPDATE", "DELETE"))
                else "create")
        writes.append((target, mode))
    wset = {w for w, _ in writes}
    reads = [r for r in SQL_READ_RE.findall(clean) if r not in wset and "information_schema" not in r.lower()]
    return reads, writes


def fingerprint(sql: str | None) -> str | None:
    if not sql:
        return None
    norm = re.sub(r"\s+", " ", re.sub(r"'[^']*'|\b\d+\b", "?", sql.lower())).strip()
    return hashlib.sha1(norm.encode()).hexdigest()[:12]


def _parse_dt(v: Any) -> datetime | None:
    if isinstance(v, datetime):
        return v if v.tzinfo else v.replace(tzinfo=timezone.utc)
    if not v:
        return None
    try:
        return datetime.fromisoformat(str(v).replace("Z", "+00:00"))
    except ValueError:
        return None


def _human_interval(seconds: float | None) -> str | None:
    if not seconds:
        return None
    for unit, s in (("día(s)", 86400), ("hora(s)", 3600), ("minuto(s)", 60)):
        if seconds >= s * 0.9:
            return f"cada ~{seconds / s:.0f} {unit}"
    return f"cada ~{seconds:.0f} s"


# ============================================================ evaluación de tablas
def assess_table(t: dict[str, Any]) -> dict[str, Any]:
    cols = t.get("columns") or []
    names = [c["name"] for c in cols]
    layer, layer_src = classify_layer(t.get("table"), t.get("dataset"),
                                      {**(t.get("dataset_labels") or {}), **(t.get("labels") or {})})
    size_gb = round(float(t.get("logical_bytes") or 0) / P.GIB, 3)

    fk_like = [n for n in names if re.search(r"(_id|_key|_fk|_sk)$|^id_|^fk_", n, re.I) and n.lower() not in ("id",)]
    pairs = []
    for fk in fk_like:
        base = re.sub(r"(_id|_key|_fk|_sk)$|^(id_|fk_)", "", fk, flags=re.I).lower()
        if base and any(n.lower().startswith(base) and n.lower().endswith(DESC_SUFFIX) and n.lower() != fk.lower()
                        for n in names):
            pairs.append(base)
    groups = Counter(re.sub(r"_?\d+$", "", n.lower()) for n in names if re.search(r"[a-z]_?\d+$", n.lower()))
    repeating = sorted(g for g, c in groups.items() if c >= 2)
    nested = sum(1 for c in cols if (c.get("type") or "").upper().startswith(("RECORD", "STRUCT")))
    repeated = sum(1 for c in cols if (c.get("mode") or "").upper() == "REPEATED" or (c.get("type") or "").upper().startswith("ARRAY"))
    str_dates = [c["name"] for c in cols if (c.get("type") or "").upper() == "STRING" and DATE_NAME_RE.search(c["name"])]
    str_nums = [c["name"] for c in cols if (c.get("type") or "").upper() == "STRING" and NUMERIC_NAME_RE.search(c["name"])]
    pii = [n for n in names if PII_RE.search(n)]

    denorm = len(pairs) + 2 * len(repeating) + (nested > 0) + (repeated > 0) + (len(cols) > 60)
    norm_hint = len(fk_like) >= 2 and not pairs and len(cols) <= 25 and not nested
    normalization = ("desnormalizada" if denorm >= 2 else "normalizada" if norm_hint
                     else "mixta" if cols else "n/d")
    expected = {"bronze": "espejo del origen (sin transformar)", "silver": "normalizada / entidades conformadas",
                "gold": "desnormalizada (modelo estrella o tablas anchas para consumo)"}.get(layer or "", "n/d")

    signals: list[dict[str, Any]] = []

    def sig(code: str, severity: str, text: str) -> None:
        signals.append({"code": code, "severity": severity, "text": text})

    is_table = (t.get("type") or "BASE TABLE") == "BASE TABLE"
    if not layer:
        sig("LAYER_UNDECLARED", "LOW", "La tabla no declara su capa (ni etiqueta ni convención de nombre).")
    if layer == "silver" and normalization == "desnormalizada":
        sig("NORMALIZATION_ISSUE", "MEDIUM", f"Silver desnormalizada: repite atributos descriptivos ({', '.join(pairs[:5])}) "
                                             "o grupos repetidos; en silver se esperan entidades conformadas.")
    if layer == "gold" and normalization == "normalizada":
        sig("NORMALIZATION_ISSUE", "MEDIUM", f"Gold normalizada ({len(fk_like)} claves foráneas, sin atributos "
                                             "descriptivos): obliga a los consumidores a hacer joins.")
    if repeating and layer in ("silver", "gold"):
        sig("NORMALIZATION_ISSUE", "MEDIUM", f"Grupos repetidos de columnas (viola 1FN): {', '.join(repeating[:5])}.")
    if str_dates:
        sig("SCHEMA_TYPE_ISSUE", "MEDIUM" if layer in ("silver", "gold") else "LOW",
            f"Fechas guardadas como STRING: {', '.join(str_dates[:6])}.")
    if str_nums:
        sig("SCHEMA_TYPE_ISSUE", "MEDIUM" if layer in ("silver", "gold") else "LOW",
            f"Valores numéricos guardados como STRING: {', '.join(str_nums[:6])}.")
    if is_table and size_gb >= 1 and not t.get("partition_column"):
        sig("NO_PARTITIONING", "HIGH" if size_gb >= 50 else "MEDIUM",
            f"{size_gb:,.1f} GB sin particionar: cada consulta escanea la tabla completa.")
    if is_table and size_gb >= 10 and not t.get("clustering"):
        sig("NO_CLUSTERING", "LOW", f"{size_gb:,.1f} GB sin clustering.")
    if pii and layer in ("silver", "gold", None):
        sig("PII_COLUMNS", "MEDIUM", f"Columnas con posibles datos personales: {', '.join(pii[:6])} "
                                     "(verificar policy tags / enmascaramiento).")
    if not t.get("description"):
        sig("UNDOCUMENTED", "LOW", "Sin descripción de tabla.")
    last_mod = _parse_dt(t.get("last_modified"))
    days_since = (datetime.now(timezone.utc) - last_mod).days if last_mod else None

    storage = P.bq_storage_monthly(float(t.get("active_logical") or 0), float(t.get("lt_logical") or 0),
                                   float(t.get("active_physical") or 0), float(t.get("lt_physical") or 0))
    return {
        "resource_id": f"bq:{t['project']}.{t['dataset']}.{t['table']}", "project_id": t["project"],
        "dataset": t["dataset"], "table": t["table"], "type": t.get("type"), "layer": layer, "layer_source": layer_src,
        "size_gb": size_gb, "rows": t.get("rows"), "partitioned": bool(t.get("partition_column")),
        "partition_column": t.get("partition_column"), "clustering": t.get("clustering") or [],
        "columns_count": len(cols), "nested_fields": nested, "repeated_fields": repeated,
        "fk_like_columns": fk_like[:10], "id_description_pairs": pairs[:10], "repeating_groups": repeating[:10],
        "normalization": normalization, "expected_for_layer": expected,
        "string_date_columns": str_dates[:10], "numeric_as_string_columns": str_nums[:10], "pii_columns": pii[:10],
        "description_present": bool(t.get("description")), "days_since_modified": days_since,
        "storage_monthly_usd": storage["logical"], "signals": signals,
        "writers": [], "readers": [], "consumer_queries_30d": 0,
    }


def assess_datasets(datasets: list[dict[str, Any]], tables: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for d in datasets:
        ts = [t for t in tables if t["project"] == d["project"] and t["dataset"] == d["dataset"]]
        agg = {k: sum(float(t.get(k) or 0) for t in ts) for k in ("active_logical", "lt_logical", "active_physical", "lt_physical")}
        cost = P.bq_storage_monthly(agg["active_logical"], agg["lt_logical"], agg["active_physical"], agg["lt_physical"])
        model = (d.get("storage_billing_model") or "LOGICAL").upper()
        current = cost["physical"] if model == "PHYSICAL" else cost["logical"]
        best = min(cost, key=cost.get)
        saving = round(current - cost[best], 2)
        layer, src = classify_layer(d["dataset"], None, d.get("labels"))
        out.append({
            "resource_id": f"bqds:{d['project']}.{d['dataset']}", "project_id": d["project"], "dataset": d["dataset"],
            "location": d.get("location"), "layer": layer, "layer_source": src, "tables": len(ts),
            "size_gb": round(agg["active_logical"] / P.GIB + agg["lt_logical"] / P.GIB, 2),
            "storage_billing_model": model, "storage_monthly_usd": current,
            "recommended_billing_model": best.upper() if saving > 5 and saving / max(current, 1) > 0.1 else model,
            "billing_model_saving_usd": saving if saving > 5 and saving / max(current, 1) > 0.1 else 0.0,
            "layers_mixed": sorted({t2 for t2 in (classify_layer(t["table"], None, t.get("labels"))[0] for t in ts) if t2}),
        })
    return out


# ============================================================ evaluación del data lake
def assess_bucket(b: dict[str, Any]) -> dict[str, Any]:
    layer, src = classify_layer(b["bucket"], None, b.get("labels"))
    ext_bytes: dict[str, float] = b.get("ext_bytes") or {}
    total = sum(ext_bytes.values()) or 0.0
    text_share = sum(v for k, v in ext_bytes.items() if k in TEXT_FORMATS) / total if total else 0.0
    dominant = max(ext_bytes, key=ext_bytes.get) if ext_bytes else None
    prefixes = []
    for p in b.get("top_prefixes") or []:
        pl, _ = classify_layer(p, None, None)
        prefixes.append({"prefix": p, "layer": pl})
    layers_inside = sorted({p["layer"] for p in prefixes if p["layer"]})
    signals: list[dict[str, Any]] = []

    def sig(code: str, severity: str, text: str) -> None:
        signals.append({"code": code, "severity": severity, "text": text})

    size_gb = float(b.get("size_bytes") or 0) / P.GIB
    effective_layer = layer or (layers_inside[0] if len(layers_inside) == 1 else None)
    if text_share > 0.5 and total > 1 * P.GIB and effective_layer in ("silver", "gold", None) and layers_inside != ["bronze"]:
        sig("WRONG_FILE_FORMAT", "MEDIUM", f"{text_share:.0%} del volumen muestreado está en formatos de texto "
                                           f"({dominant}); para analítica usar Parquet/Avro (columnar, comprimido, con esquema).")
    if b.get("small_files_pct") is not None and b["small_files_pct"] > 70 and (b.get("sampled_objects") or 0) >= 100:
        sig("SMALL_FILES", "MEDIUM", f"{b['small_files_pct']:.0f}% de los archivos pesan < 32 MB "
                                     f"(promedio {b.get('avg_object_mb', 0):.1f} MB): problema de archivos pequeños; compactar.")
    sc = (b.get("storage_class") or "STANDARD").upper()
    old = b.get("median_age_days")
    reqs = b.get("requests_30d")
    if sc in ("STANDARD", "MULTI_REGIONAL", "REGIONAL") and old is not None and old > 90 and not b.get("lifecycle_rules") \
            and (reqs is None or reqs < 1000):
        saving = P.gcs_monthly(float(b.get("size_bytes") or 0), sc) - P.gcs_monthly(float(b.get("size_bytes") or 0), "NEARLINE")
        sig("WRONG_STORAGE_CLASS", "MEDIUM", f"Objetos con mediana de {old:.0f} días en {sc} y poco acceso "
                                             f"({reqs if reqs is not None else 'N/D'} peticiones/30 días): una regla de ciclo de vida "
                                             f"a Nearline/Coldline ahorraría ~${saving:,.2f}/mes.")
    if sc in ("COLDLINE", "ARCHIVE") and reqs is not None and reqs > 10000:
        sig("WRONG_STORAGE_CLASS", "HIGH", f"Clase {sc} con {reqs:,} peticiones en 30 días: las tarifas de recuperación "
                                           "superan el ahorro; usar STANDARD.")
    if not b.get("lifecycle_rules") and (effective_layer == "bronze" or "bronze" in layers_inside or
                                         any(t in _tokens(b["bucket"]) for t in ("tmp", "temp", "staging", "logs"))):
        sig("NO_LIFECYCLE", "LOW", "Sin reglas de ciclo de vida en una zona de aterrizaje/temporal.")
    if (b.get("sampled_objects") or 0) >= 200 and not b.get("partition_style") and effective_layer in ("bronze", "silver", None):
        sig("NO_PARTITION_LAYOUT", "LOW", "Rutas sin particionado tipo Hive (p.ej. dt=AAAA-MM-DD/): dificulta "
                                          "lecturas incrementales y tablas externas particionadas.")
    if len(layers_inside) > 1:
        sig("MIXED_LAYERS", "LOW", f"El bucket mezcla capas ({', '.join(layers_inside)}); separar por bucket facilita "
                                   "permisos, ciclo de vida y costos por capa.")
    return {
        "resource_id": f"gcs:{b['bucket']}", "project_id": b.get("project_id"), "bucket": b["bucket"],
        "storage_class": sc, "location": b.get("location"), "layer": layer, "layer_source": src,
        "layers_in_prefixes": layers_inside, "top_prefixes": prefixes[:15], "size_gb": round(size_gb, 2),
        "monthly_usd": P.gcs_monthly(float(b.get("size_bytes") or 0), sc),
        "formats_by_bytes": {k: round(v / total * 100, 1) for k, v in sorted(ext_bytes.items(), key=lambda kv: -kv[1])[:8]} if total else {},
        "dominant_format": dominant, "sampled_objects": b.get("sampled_objects"),
        "avg_object_mb": b.get("avg_object_mb"), "small_files_pct": b.get("small_files_pct"),
        "partition_style": b.get("partition_style"), "median_age_days": old, "requests_30d": reqs,
        "lifecycle_rules": b.get("lifecycle_rules") or 0, "versioning": b.get("versioning"), "signals": signals,
    }


# ============================================================ flujos
class FlowBuilder:
    def __init__(self, project_id: str, history_days: int, self_email: str | None) -> None:
        self.project_id = project_id
        self.days = history_days
        self.self_email = (self_email or "").lower()
        self.flows: dict[str, dict[str, Any]] = {}
        self.data_nodes: dict[str, dict[str, Any]] = {}
        self.table_consumers: Counter = Counter()
        self.transfer_identity: dict[str, str] = {}
        self.transfer_kind: dict[str, str] = {}

    # -------- helpers
    def data(self, ref: str | dict[str, Any] | None) -> dict[str, Any] | None:
        node = normalize_ref(ref, self.project_id) if isinstance(ref, str) else ref
        if not node:
            return None
        self.data_nodes.setdefault(node["id"], dict(node))
        return self.data_nodes[node["id"]]

    def flow(self, key: str, kind: str, name: str, **attrs: Any) -> dict[str, Any]:
        fid = f"flow:{self.project_id}:{kind}:{key}"[:220]
        f = self.flows.get(fid)
        if f is None:
            f = self.flows[fid] = {
                "resource_id": fid, "project_id": self.project_id, "name": name, "kind": kind,
                "engine": attrs.pop("engine", kind), "inputs": {}, "outputs": {}, "schedule": None, "identity": None,
                "runs": [], "declared_state": None, "cost_30d_usd": 0.0, "_window_cost": 0.0, "_monthly_cost": 0.0, "cost_basis": [], "signals": [],
                "fingerprints": set(), "sample_query": None, "description": None, "owner": None,
                "bytes_processed_30d": 0, "slot_hours_30d": 0.0, "labels": {}, "display_name": None,
            }
        for k, v in attrs.items():
            if v is not None:
                f[k] = v
        return f

    def add_io(self, f: dict[str, Any], ref: Any, direction: str, mode: str | None = None, **meta: Any) -> None:
        node = self.data(ref)
        if not node:
            return
        bucket = f["inputs"] if direction == "in" else f["outputs"]
        entry = bucket.setdefault(node["id"], {"id": node["id"], "modes": set(), **meta})
        if isinstance(ref, str) and not entry.get("raw"):
            entry["raw"] = ref.strip().strip("`")
        if mode:
            entry["modes"].add(mode)

    def add_run(self, f: dict[str, Any], start: Any, end: Any, ok: bool, error: str | None = None, **extra: Any) -> None:
        s, e = _parse_dt(start), _parse_dt(end)
        f["runs"].append({"start": s, "end": e, "ok": ok, "error": error,
                          "duration_s": (e - s).total_seconds() if s and e else None, **extra})

    # -------- BigQuery jobs
    def add_bq_jobs(self, jobs: list[dict[str, Any]], details: dict[str, dict[str, Any]],
                    transfers_by_dest: dict[str, str]) -> None:
        # El colector consulta el detalle de UN job por (identidad, destino, tipo): su write_disposition vale para todos.
        sampled: dict[tuple, dict[str, Any]] = {}
        for j in jobs:
            if j["job_id"] in details:
                sampled[(j.get("user_email"), j.get("destination"), j.get("job_type"))] = details[j["job_id"]]
        for j in jobs:
            j = dict(j)
            j["job_id"] = safe_str(j.get("job_id")) or ""
            refs = [r for r in safe_list(j.get("referenced")) if isinstance(r, str) and r.count(".") >= 2]
            for r in refs:
                self.table_consumers[f"data:bq:{r}"] += 1
            if (j.get("user_email") or "").lower() == self.self_email:
                continue
            dest = safe_str(j.get("destination"))
            if not dest or dest.count(".") < 2 or dest.split(".")[1].startswith("_"):
                continue  # consulta ad-hoc/BI o sin destino detectable: sólo cuenta como consumidor
            labels = safe_labels(j.get("labels"))
            parent = safe_str(j.get("parent_job_id")) or ""
            tkey = transfers_by_dest.get(dest)
            from_dts = bool(tkey) and (bool(labels.get("data_source_id")) or j["job_id"].startswith("scheduled_query")
                                       or parent.startswith("scheduled_query")
                                       or (j.get("user_email") or "") == self.transfer_identity.get(tkey or ""))
            stmt = (j.get("statement_type") or j.get("job_type") or "").upper()
            detail = details.get(j["job_id"]) or sampled.get((j.get("user_email"), dest, j.get("job_type")), {})
            if stmt in ("INSERT",):
                mode = "append"
            elif stmt == "MERGE":
                mode = "merge"
            elif stmt in ("UPDATE", "DELETE"):
                mode = "update"
            elif stmt in ("CREATE_TABLE_AS_SELECT", "TRUNCATE_TABLE", "CREATE_TABLE"):
                mode = "overwrite" if "REPLACE" in (j.get("query") or "").upper() or stmt == "TRUNCATE_TABLE" else "create"
            else:
                wd = (detail.get("write_disposition") or "").upper()
                mode = {"WRITE_TRUNCATE": "overwrite", "WRITE_APPEND": "append", "WRITE_EMPTY": "create"}.get(wd, "unknown")
            dag = labels.get("airflow-dag") or labels.get("airflow_dag")
            if dag:
                f = self.flow(f"composer:{dag}", "composer_dag", dag, engine="Composer/Airflow → BigQuery")
            elif from_dts:
                f = self.flows[f"flow:{self.project_id}:{self.transfer_kind.get(tkey, 'bq_scheduled_query')}:{tkey}"[:220]]
            elif j["job_id"].startswith("scheduled_query") or parent.startswith("scheduled_query") \
                    or labels.get("data_source_id") == "scheduled_query":
                f = self.flow(f"dts:{dest}", "bq_scheduled_query", dest.split(".")[-1], engine="BigQuery Scheduled Query")
            elif labels.get("dataform") or "dataform" in (j.get("user_email") or ""):
                f = self.flow(f"dataform:{dest}", "dataform", dest, engine="Dataform")
            elif labels.get("dbt_invocation_id") or any(k.startswith("dbt") for k in labels):
                f = self.flow(f"dbt:{dest.split('.')[1]}", "dbt", f"dbt {dest.split('.')[1]}", engine="dbt → BigQuery")
            else:
                kind = "bq_load" if j.get("job_type") == "LOAD" else "bq_query"
                f = self.flow(f"{j.get('user_email')}|{dest}", kind, f"{'Carga' if kind == 'bq_load' else 'Consulta'} → {dest}",
                              engine="BigQuery")
            f["identity"] = f["identity"] or j.get("user_email")
            f["labels"].update({k: v for k, v in labels.items() if k not in ("data_source_id",)})
            self.add_io(f, dest, "out", mode)
            for r in refs:
                if r != dest:
                    self.add_io(f, r, "in")
            for uri in safe_list(detail.get("source_uris")):
                self.add_io(f, uri, "in", format=(detail.get("source_format") or "").lower() or fmt_from_path(uri))
            f["bytes_processed_30d"] += int(j.get("bytes_billed") or 0)
            f["slot_hours_30d"] += float(j.get("slot_ms") or 0) / 3.6e6
            f["_window_cost"] += P.bq_query_cost(j.get("bytes_billed"), j.get("slot_ms"))
            if "BigQuery (bytes/slots)" not in f["cost_basis"]:
                f["cost_basis"].append("BigQuery (bytes/slots)")
            if j.get("query"):
                fp = fingerprint(j["query"])
                f["fingerprints"].add(fp)
                f["sample_query"] = f["sample_query"] or j["query"][:600]
            self.add_run(f, j.get("creation_time"), j.get("end_time"), j.get("state") == "DONE" and not j.get("error_reason"),
                         j.get("error_reason"), bytes=int(j.get("bytes_billed") or 0))

    # -------- Data Transfer (scheduled queries, GCS→BQ, PostgreSQL→BQ…)
    def add_transfers(self, transfers: list[dict[str, Any]]) -> dict[str, str]:
        by_dest: dict[str, str] = {}
        for t in transfers:
            name = safe_str(t.get("name"))
            if not name:
                continue
            params = t.get("params") or {}
            ds = safe_str(t.get("destination_dataset"))
            kind = "bq_scheduled_query" if t.get("data_source_id") == "scheduled_query" else "dts"
            key = f"dts:{name.split('/')[-1]}"
            display = safe_str(t.get("display_name")) or name.split("/")[-1]
            f = self.flow(key, kind, re.sub(r"^\s*\[[^\]]+\]\s*", "", display) or display,
                          engine=f"BigQuery Data Transfer ({t.get('data_source_id') or 'desconocido'})",
                          schedule=safe_str(t.get("schedule")), display_name=display,
                          declared_state="disabled" if t.get("disabled") else (safe_str(t.get("state")) or "").lower() or None,
                          identity=safe_str(t.get("service_account")))
            self.transfer_kind[key] = kind
            if f["identity"]:
                self.transfer_identity[key] = f["identity"]
            q = safe_str(params.get("query"))
            f["labels"].update(query_label_from_sql(q))
            wd = (safe_str(params.get("write_disposition")) or "").upper()
            mode = {"WRITE_TRUNCATE": "overwrite", "WRITE_APPEND": "append", "APPEND": "append",
                    "MIRROR": "overwrite", "WRITE_EMPTY": "create"}.get(wd)
            writes: list[tuple[str, str]] = []
            if q:
                reads, writes = sql_io(q, self.project_id)
                for r in reads:
                    self.add_io(f, r, "in")
                for w, m in writes:
                    self.add_io(f, w, "out", m)
                f["fingerprints"].add(fingerprint(q))
                f["sample_query"] = q[:600]
            tmpl = params.get("destination_table_name_template")
            for w, _ in writes:  # DML (MERGE/INSERT): el destino real es el del SQL
                node = normalize_ref(w, self.project_id)
                if node:
                    by_dest[node["name"]] = key
            if tmpl and ds and not writes:  # SELECT + tabla destino: el modo lo da write_disposition
                target = f"{self.project_id}.{ds}.{re.sub(r'[{$].*$', '', tmpl) or tmpl}"
                self.add_io(f, target, "out", mode or "unknown")
                by_dest[target] = key
            src = safe_str(params.get("data_path_template")) or safe_str(params.get("data_path"))
            if src:
                self.add_io(f, src, "in", format=(params.get("file_format") or "").lower() or None)
                if ds and params.get("destination_table_name_template"):
                    pass
            if t.get("data_source_id") in ("postgresql", "mysql", "oracle", "sqlserver"):
                host = params.get("connector.endpoint.host") or params.get("host") or "origen"
                db = params.get("connector.database") or params.get("database") or ""
                self.add_io(f, {"id": f"data:{t['data_source_id'].replace('postgresql', 'postgres')}:{host}/{db}",
                                "system": t["data_source_id"], "name": f"{t['data_source_id']}://{host}/{db}"}, "in")
                if ds:
                    self.add_io(f, {"id": f"data:bqds:{self.project_id}.{ds}", "system": "bigquery",
                                    "name": f"{self.project_id}.{ds} (dataset)"}, "out", mode or "unknown")
            for r in t.get("runs") or []:
                self.add_run(f, r.get("start"), r.get("end"), (r.get("state") or "") == "SUCCEEDED", r.get("error"))
        return by_dest

    # -------- Vistas lógicas (INFORMATION_SCHEMA.VIEWS)
    def add_views(self, views: list[dict[str, Any]], table_labels: dict[str, dict[str, str]]) -> None:
        for v in views:
            ds, name = safe_str(v.get("dataset")), safe_str(v.get("view"))
            if not ds or not name:
                continue
            target = f"{v.get('project') or self.project_id}.{ds}.{name}"
            f = self.flow(f"view:{target}", "bq_view", name, engine="BigQuery View (lógica)")
            f["labels"].update(table_labels.get(f"bq:{target}", {}))
            f["sample_query"] = (safe_str(v.get("definition")) or "")[:600] or None
            reads, _ = sql_io(v.get("definition") or "", self.project_id)
            for r in reads:
                self.add_io(f, r, "in")
            self.add_io(f, target, "out", "view")

    # -------- Dataflow
    def add_dataflow(self, jobs: list[dict[str, Any]]) -> None:
        for j in jobs:
            base = re.sub(r"[-_]?\d{6,}.*$|[-_][0-9a-f]{8,}$", "", j.get("name") or j["id"]) or j["name"]
            streaming = "STREAM" in (j.get("type") or "").upper()
            f = self.flow(f"dataflow:{base}", "dataflow", base,
                          engine=f"Dataflow ({'streaming' if streaming else 'batch'})",
                          identity=j.get("service_account"))
            f["streaming"] = streaming
            for r in j.get("inputs") or []:
                self.add_io(f, r, "in")
            for r in j.get("outputs") or []:
                self.add_io(f, r, "out", "append" if streaming else None)
            for r in j.get("io_unknown") or []:
                self.add_io(f, r, "out" if streaming and str(r).count(".") == 2 else "in")
            state = (j.get("state") or "").upper()
            ok = state in ("JOB_STATE_DONE", "JOB_STATE_RUNNING", "JOB_STATE_UPDATED", "JOB_STATE_DRAINED")
            self.add_run(f, j.get("start_time") or j.get("create_time"),
                         None if state == "JOB_STATE_RUNNING" else j.get("end_time"), ok,
                         None if ok else state, state=state)
            if j.get("vcpu_s") is not None:
                cost = P.dataflow_cost(float(j["vcpu_s"]), float(j.get("mem_gb_s") or 0), streaming)
                if streaming and j.get("elapsed_s"):
                    f["_monthly_cost"] += cost / max(float(j["elapsed_s"]), 3600) * 30 * 86400  # proyección mensual
                else:
                    f["_window_cost"] += cost
                if "Dataflow (vCPU/memoria)" not in f["cost_basis"]:
                    f["cost_basis"].append("Dataflow (vCPU/memoria)")
            if state == "JOB_STATE_RUNNING":
                f["declared_state"] = "running"

    # -------- Dataproc
    def add_dataproc_jobs(self, jobs: list[dict[str, Any]], cluster_costs: dict[str, float]) -> dict[str, float]:
        """Reparte el costo del cluster entre sus jobs por tiempo de ejecución. Devuelve costo ocioso por cluster."""
        seconds_by_cluster: dict[str, float] = defaultdict(float)
        for j in jobs:
            s, e = _parse_dt(j.get("start")), _parse_dt(j.get("end"))
            j["_dur"] = (e - s).total_seconds() if s and e else 0.0
            seconds_by_cluster[j.get("cluster") or "serverless"] += j["_dur"]
        window_s = self.days * 86400
        for j in jobs:
            main = j.get("main") or j.get("type") or "job"
            key = re.sub(r"^.*/", "", main)
            f = self.flow(f"dataproc:{j.get('cluster') or 'serverless'}:{key}", "dataproc", key,
                          engine=f"Dataproc {j.get('type', '')} en {j.get('cluster') or 'Serverless'}".strip(),
                          cluster=j.get("cluster"))
            for r in j.get("inputs") or []:
                self.add_io(f, r, "in")
            for r in j.get("outputs") or []:
                self.add_io(f, r, "out", j.get("write_mode"))
            for r in j.get("io") or []:
                self.add_io(f, r, "in")
            ok = (j.get("state") or "").upper() in ("DONE", "SUCCEEDED", "RUNNING")
            self.add_run(f, j.get("start"), j.get("end"), ok, None if ok else j.get("state"))
            cl = j.get("cluster")
            if cl and cl in cluster_costs and seconds_by_cluster[cl] > 0:
                window_cost = cluster_costs[cl] * self.days / 30
                f["_monthly_cost"] += window_cost * j["_dur"] / max(seconds_by_cluster[cl], window_s) * 30 / self.days
                if "Dataproc (prorrateo del cluster)" not in f["cost_basis"]:
                    f["cost_basis"].append("Dataproc (prorrateo del cluster)")
            elif not cl and j.get("dcu_hours"):
                f["_window_cost"] += float(j["dcu_hours"]) * P.DATAPROC_SERVERLESS_DCU_H
        idle = {}
        for cl, monthly in cluster_costs.items():
            used_share = min(1.0, seconds_by_cluster.get(cl, 0.0) / window_s)
            idle[cl] = round(monthly * (1 - used_share), 2)
        return idle

    # -------- Composer DAGs (parseados del código)
    def add_dags(self, dags: list[dict[str, Any]]) -> None:
        for d in dags:
            f = self.flow(f"composer:{d['dag_id']}", "composer_dag", d["dag_id"], engine="Composer/Airflow",
                          schedule=d.get("schedule"), environment=d.get("environment"),
                          operators=sorted(set(d.get("operators") or []))[:20], source_file=d.get("file"))
            for r in d.get("inputs") or []:
                self.add_io(f, r, "in")
            for r, mode in (d.get("outputs") or {}).items():
                self.add_io(f, r, "out", mode)
            if d.get("is_paused"):
                f["declared_state"] = "paused"

    # -------- Datastream (CDC desde Postgres/MySQL/Oracle)
    def add_datastream(self, streams: list[dict[str, Any]]) -> None:
        for s in streams:
            f = self.flow(f"datastream:{s['name']}", "datastream", s["name"], engine="Datastream (CDC)",
                          declared_state=(s.get("state") or "").lower() or None)
            if s.get("source"):
                self.add_io(f, s["source"], "in")
            if s.get("destination"):
                self.add_io(f, s["destination"], "out", "merge" if s.get("mode") == "merge" else "append")

    # -------- Microservicios (Cloud Run / Functions) a partir del grafo de arquitectura
    def add_services(self, services: list[dict[str, Any]]) -> None:
        for s in services:
            f = self.flow(f"svc:{s['name']}", "microservice", s["name"], engine=s.get("service_type", "Cloud Run"),
                          identity=s.get("runtime_service_account"))
            for r in s.get("inputs") or []:
                self.add_io(f, r, "in")
            for r in s.get("outputs") or []:
                self.add_io(f, r, "out")
            f["requests_30d"] = s.get("request_count_30d")
            if s.get("cost_30d_usd") is not None:
                f["_monthly_cost"] += float(s["cost_30d_usd"])
                f["cost_basis"].append(s.get("cost_basis") or "Cloud Run (tiempo facturable)")
            if s.get("request_count_30d") == 0:
                f["declared_state"] = "no_traffic"

    # -------- Catálogo aportado por el usuario (fuentes on-prem, HDFS, Postgres…)
    def add_catalog(self, catalog: list[dict[str, Any]]) -> None:
        for c in catalog:
            name = c.get("name") or c.get("id")
            if not name:
                continue
            existing = next((f for f in self.flows.values() if f["name"] == name), None)
            f = existing or self.flow(f"catalog:{name}", "catalog", name, engine=c.get("engine") or c.get("type") or "declarado")
            f["description"] = c.get("description") or f["description"]
            f["owner"] = c.get("owner") or f["owner"]
            f["schedule"] = c.get("schedule") or f["schedule"]
            for i in c.get("inputs") or []:
                ref = i if isinstance(i, str) else self._catalog_ref(i)
                meta = {} if isinstance(i, str) else {k: i.get(k) for k in ("format", "structure", "declared_layer") if i.get(k)}
                self.add_io(f, ref, "in", **meta)
            for o in c.get("outputs") or []:
                ref = o if isinstance(o, str) else self._catalog_ref(o)
                mode = None if isinstance(o, str) else (o.get("write_mode") or o.get("mode"))
                meta = {} if isinstance(o, str) else {k: o.get(k) for k in ("format", "structure", "declared_layer") if o.get(k)}
                self.add_io(f, ref, "out", mode, **meta)

    def _catalog_ref(self, item: dict[str, Any]) -> Any:
        name = item.get("name") or item.get("path") or item.get("table") or ""
        system = (item.get("system") or "").lower()
        node = normalize_ref(name, self.project_id)
        if node:
            if item.get("layer"):
                node["declared_layer"] = item["layer"]
            return node
        sid = system or "ext"
        return {"id": f"data:{sid}:{name}", "system": sid, "name": name, "format": item.get("format"),
                "declared_layer": item.get("layer")}


# ============================================================ análisis de flujos
def _finalize_flow(f: dict[str, Any], days: int) -> None:
    runs = sorted([r for r in f["runs"] if r["start"]], key=lambda r: r["start"])
    f["runs_in_window"] = len(runs)
    f["failures"] = sum(1 for r in runs if not r["ok"])
    f["failure_rate_pct"] = round(100 * f["failures"] / len(runs), 1) if runs else None
    f["last_run"] = runs[-1]["start"].isoformat() if runs else None
    f["last_status"] = ("ok" if runs[-1]["ok"] else f"error: {runs[-1]['error']}") if runs else None
    durs = [r["duration_s"] for r in runs if r["duration_s"] is not None]
    f["avg_duration_s"] = round(statistics.mean(durs), 1) if durs else None
    f["p95_duration_s"] = round(sorted(durs)[max(0, int(len(durs) * 0.95) - 1)], 1) if durs else None
    f["max_duration_s"] = round(max(durs), 1) if durs else None
    if len(durs) >= 6:
        third = len(durs) // 3
        first, last = statistics.median(durs[:third]), statistics.median(durs[-third:])
        f["duration_trend_pct"] = round((last - first) / first * 100, 1) if first else None
    else:
        f["duration_trend_pct"] = None
    starts = [r["start"] for r in runs]
    gaps = [(b - a).total_seconds() for a, b in zip(starts, starts[1:]) if (b - a).total_seconds() > 60]
    f["typical_interval_s"] = statistics.median(gaps) if gaps else None
    f["schedule"] = f["schedule"] or _human_interval(f["typical_interval_s"])
    now = datetime.now(timezone.utc)
    days_since = (now - runs[-1]["start"]).days if runs else None
    f["days_since_last_run"] = days_since

    ds = f.get("declared_state")
    if ds in ("disabled", "paused", "no_traffic", "not_started"):
        state = "inactive"
    elif ds in ("failed", "failed_permanently"):
        state = "failing"
    elif ds == "running" or (f.get("streaming") and runs and not runs[-1]["end"]):
        state = "active"
    elif not runs:
        state = "inactive" if f["kind"] not in ("microservice", "catalog") else "unknown"
    elif days_since is not None and days_since > max(7, 3 * (f["typical_interval_s"] or 0) / 86400):
        state = "inactive"
    elif (f["failure_rate_pct"] or 0) >= 20 or not runs[-1]["ok"]:
        state = "failing"
    else:
        state = "active"
    if f["kind"] == "microservice" and f.get("requests_30d"):
        state = "active"
    if f["kind"] == "bq_view":
        state = "unknown"   # una vista no se "ejecuta": su uso se ve en las consultas que la leen
    f["state"] = state
    f["state_es"] = ("vista lógica" if f["kind"] == "bq_view" else
                     {"active": "activo", "inactive": "inactivo", "failing": "con fallos", "unknown": "desconocido"}[state])
    f["cost_30d_usd"] = round(f.pop("_window_cost") * 30 / max(days, 1) + f.pop("_monthly_cost"), 2)
    f["runs"] = [{"start": r["start"].isoformat(), "duration_s": r["duration_s"], "ok": r["ok"]} for r in runs[-10:]]


def _io_list(f: dict[str, Any], key: str, nodes: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for nid, e in f[key].items():
        n = nodes.get(nid, {})
        modes = sorted(e.get("modes") or [])
        for weak in ("unknown", "create"):     # modos poco informativos ceden ante uno concreto (p.ej. CREATE OR REPLACE)
            if len(modes) > 1 and weak in modes:
                modes.remove(weak)
        out.append({"id": nid, "raw": e.get("raw"), "name": n.get("name", nid), "system": n.get("system"), "layer": n.get("layer"),
                    "format": e.get("format") or n.get("format"), "structure": e.get("structure"),
                    "write_mode": (modes[0] if len(modes) == 1 else ("+".join(modes) if modes else None)) if key == "outputs" else None})
    return out


def detect_issues(flows: list[dict[str, Any]], nodes: dict[str, dict[str, Any]], tables: dict[str, dict[str, Any]],
                  consumers: Counter) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []

    def issue(code: str, severity: str, resource_id: str, text: str, flows_: list[str] | None = None) -> None:
        issues.append({"code": code, "severity": severity, "resource_id": resource_id, "text": text,
                       "flows": flows_ or []})

    writers: dict[str, list[tuple[dict[str, Any], str]]] = defaultdict(list)
    readers: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for f in flows:
        for o in f["outputs"]:
            writers[o["id"]].append((f, o.get("write_mode") or "unknown"))
        for i in f["inputs"]:
            readers[i["id"]].append(f)

    # --- conflictos: varios escritores sobre el mismo destino
    for did, ws in writers.items():
        live = [(f, m) for f, m in ws if f["state"] != "inactive"]
        if len({f["resource_id"] for f, _ in live}) < 2:
            continue
        modes = {m for _, m in live}
        names = [f["name"] for f, _ in live]
        ids = [f["resource_id"] for f, _ in live]
        target = nodes.get(did, {}).get("name", did)
        if "overwrite" in modes and len(live) > 1:
            issue("FLOW_CONFLICT", "HIGH", ids[0], f"{len(live)} flujos escriben en {target} y al menos uno "
                  f"SOBRESCRIBE ({', '.join(f'{n} [{m}]' for (f, m), n in zip(live, names))}): el último en ejecutar "
                  "borra lo que escribieron los demás.", ids)
        elif len(modes - {"unknown"}) > 1:
            issue("FLOW_CONFLICT", "MEDIUM", ids[0], f"Modos de escritura contradictorios sobre {target}: "
                  f"{', '.join(f'{n} [{m}]' for (f, m), n in zip(live, names))}.", ids)
        elif modes == {"append"}:
            issue("FLOW_CONFLICT", "MEDIUM", ids[0], f"{len(live)} flujos hacen append sobre {target} "
                  f"({', '.join(names)}): riesgo de duplicados si procesan los mismos datos.", ids)

    # --- ciclos entre flujos (A escribe lo que lee B y B escribe lo que lee A…)
    graph: dict[str, set[str]] = defaultdict(set)
    for f in flows:
        for o in f["outputs"]:
            for g in readers.get(o["id"], []):
                if g["resource_id"] != f["resource_id"]:
                    graph[f["resource_id"]].add(g["resource_id"])
    color: dict[str, int] = {}
    stack: list[str] = []
    cycles: list[list[str]] = []

    def dfs(u: str) -> None:
        color[u] = 1
        stack.append(u)
        for v in graph.get(u, ()):
            if color.get(v) == 1:
                cycles.append(stack[stack.index(v):] + [v])
            elif not color.get(v):
                dfs(v)
        stack.pop()
        color[u] = 2

    for fid in list(graph):
        if not color.get(fid):
            dfs(fid)
    byid = {f["resource_id"]: f for f in flows}
    for cyc in cycles[:10]:
        issue("FLOW_CYCLE", "HIGH", cyc[0], "Dependencia circular: " + " → ".join(byid[c]["name"] for c in cyc if c in byid)
              + ". Los resultados dependen del orden de ejecución.", cyc[:-1])

    # --- redundancia: misma lógica SQL en flujos distintos
    by_fp: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for f in flows:
        for fp in f.get("fingerprints") or []:
            by_fp[fp].append(f)
    for fp, fs in by_fp.items():
        ids = sorted({f["resource_id"] for f in fs})
        if len(ids) > 1:
            issue("REDUNDANT_FLOW", "MEDIUM", ids[0], "La misma consulta se ejecuta en flujos distintos: "
                  + ", ".join(byid[i]["name"] for i in ids) + ".", ids)

    for f in flows:
        fid, name = f["resource_id"], f["name"]
        in_layers = [LAYER_ORDER.get(i.get("layer") or "", None) for i in f["inputs"]]
        in_layers = [x for x in in_layers if x is not None]
        external_in = [i for i in f["inputs"] if i["system"] in ("postgres", "mysql", "oracle", "sqlserver", "hdfs",
                                                                  "kafka", "http", "pubsub", "ext", "sftp", "api")]
        for o in f["outputs"]:
            ol = o.get("layer")
            if not ol:
                continue
            olv = LAYER_ORDER[ol]
            if in_layers and max(in_layers) > olv:
                issue("LAYER_INVERSION", "HIGH", fid, f"{name} lee de una capa superior y escribe en {ol} "
                      f"({o['name']}): el dato fluye 'hacia atrás' en la arquitectura medallion.", [fid])
            if ol in ("silver", "gold") and external_in and not [x for x in in_layers if x >= LAYER_ORDER["bronze"]]:
                issue("LAYER_SKIP", "MEDIUM", fid, f"{name} lleva datos de sistemas origen "
                      f"({', '.join(i['system'] for i in external_in[:3])}) directo a {ol} ({o['name']}) sin pasar por bronze: "
                      "se pierde el histórico crudo y la trazabilidad.", [fid])
            if ol == "gold" and in_layers and max(in_layers) == LAYER_ORDER["bronze"]:
                issue("LAYER_SKIP", "MEDIUM", fid, f"{name} construye gold ({o['name']}) directamente desde bronze, "
                      "saltándose silver (limpieza, deduplicación y conformado).", [fid])
            if ol == "bronze" and "overwrite" in (o.get("write_mode") or ""):
                issue("BRONZE_OVERWRITE", "HIGH", fid, f"{name} SOBRESCRIBE la tabla bronze {o['name']}: "
                      "bronze debería ser append-only para conservar el histórico crudo y poder reprocesar.", [fid])
            if ol == "bronze" and f.get("sample_query") and re.search(r"\bGROUP\s+BY\b|\bJOIN\b", f["sample_query"], re.I):
                issue("LAYER_MISMATCH", "MEDIUM", fid, f"{name} aplica JOIN/agregaciones antes de escribir en bronze "
                      f"({o['name']}): esa lógica corresponde a silver/gold.", [fid])
            if ol == "silver" and f.get("sample_query") and re.search(r"\bGROUP\s+BY\b", f["sample_query"], re.I) \
                    and re.search(r"\b(SUM|COUNT|AVG)\s*\(", f["sample_query"], re.I):
                issue("LAYER_MISMATCH", "LOW", fid, f"{name} agrega métricas (GROUP BY) y escribe en silver "
                      f"({o['name']}): las agregaciones de negocio suelen pertenecer a gold.", [fid])

        # --- inactividad y fallos
        if f["state"] == "inactive":
            why = {"disabled": "está deshabilitado", "paused": "está pausado",
                   "no_traffic": "no recibió tráfico en 30 días"}.get(f.get("declared_state") or "",
                  f"no se ejecuta desde hace {f['days_since_last_run']} días" if f.get("days_since_last_run") is not None
                  else "no tuvo ejecuciones en la ventana analizada")
            issue("FLOW_INACTIVE", "LOW" if f.get("declared_state") == "disabled" else "MEDIUM", fid,
                  f"{name} {why}." + (" Sigue generando costo." if f["cost_30d_usd"] > 1 else ""), [fid])
        elif f["state"] == "failing":
            issue("FLOW_FAILING", "HIGH", fid, f"{name} falla: {f.get('failure_rate_pct') or 0}% de ejecuciones con error; "
                  f"última: {f.get('last_status')}.", [fid])

        # --- rendimiento
        p95 = f.get("p95_duration_s")
        if p95 and p95 > 1800 and not f.get("streaming"):
            issue("SLOW_FLOW", "MEDIUM", fid, f"{name}: p95 de duración {p95 / 60:.0f} min "
                  f"(media {f['avg_duration_s'] / 60:.0f} min).", [fid])
        if f.get("duration_trend_pct") and f["duration_trend_pct"] > 50:
            issue("SLOW_FLOW", "MEDIUM", fid, f"{name}: su duración creció {f['duration_trend_pct']:.0f}% en la ventana "
                  "(probable crecimiento de datos sin procesamiento incremental).", [fid])
        runs = max(1, f.get("runs_in_window") or 1)
        per_run = f.get("bytes_processed_30d", 0) / runs
        in_tables = [tables.get(i["id"].replace("data:bq:", "bq:")) for i in f["inputs"]]
        in_tables = [t for t in in_tables if t]
        in_size = sum(t["size_gb"] for t in in_tables) * P.GIB
        if in_tables and in_size > 1 * P.GIB and per_run >= 0.8 * in_size and any(t["partitioned"] for t in in_tables):
            issue("FULL_SCAN", "HIGH", fid, f"{name} escanea ~{per_run / P.GIB:,.1f} GB por ejecución, prácticamente el "
                  f"100% de sus tablas de entrada aunque están particionadas: falta filtro de partición.", [fid])
        elif per_run > 100 * P.GIB:
            issue("FULL_SCAN", "MEDIUM", fid, f"{name} procesa {per_run / P.GIB:,.0f} GB por ejecución "
                  f"(~${P.bq_query_cost(per_run):,.2f} por corrida).", [fid])
        big_over = [o for o in f["outputs"] if "overwrite" in (o.get("write_mode") or "")
                    and (tables.get(o["id"].replace("data:bq:", "bq:")) or {}).get("size_gb", 0) > 10]
        if big_over and runs > 1:
            issue("FULL_RECOMPUTE", "MEDIUM", fid, f"{name} recalcula y sobrescribe tablas grandes en cada ejecución "
                  f"({', '.join(o['name'] for o in big_over)}): evaluar procesamiento incremental (MERGE por partición).", [fid])
        if f.get("sample_query") and re.search(r"SELECT\s+\*", f["sample_query"], re.I) and per_run > 10 * P.GIB:
            issue("SELECT_STAR", "LOW", fid, f"{name} usa SELECT * sobre {per_run / P.GIB:,.0f} GB por ejecución: "
                  "seleccionar sólo las columnas necesarias reduce bytes y costo.", [fid])

    # --- salidas que nadie consume
    for did, ws in writers.items():
        if did.startswith("data:bq:") and not readers.get(did) and consumers.get(did, 0) == 0:
            t = tables.get(did.replace("data:bq:", "bq:"))
            if t and t.get("layer") != "bronze":
                ids = [f["resource_id"] for f, _ in ws]
                cost_txt = ("es una vista: no ocupa almacenamiento, pero añade mantenimiento" if t.get("type") == "VIEW"
                            else f"almacenamiento ~${t['storage_monthly_usd']:,.2f}/mes")
                issue("ORPHAN_OUTPUT", "LOW", ids[0], f"{t['dataset']}.{t['table']} se produce pero nadie la leyó en la "
                      f"ventana analizada (ni flujos ni consultas): posible flujo sin propósito ({cost_txt}).", ids)
    return issues


# ============================================================ orquestación
def build_data_platform(raw: dict[str, Any], history_days: int = 30) -> dict[str, Any]:
    pid = raw.get("project_id") or ""
    tables_raw = raw.get("bq_tables") or []
    tables = {t["resource_id"]: t for t in (assess_table(x) for x in tables_raw)}
    datasets = assess_datasets(raw.get("bq_datasets") or [], tables_raw)
    lake = [assess_bucket(b) for b in raw.get("lake_buckets") or []]
    lake_by_bucket = {b["bucket"]: b for b in lake}

    fb = FlowBuilder(pid, history_days, raw.get("self_email"))
    table_labels = {f"bq:{t['project']}.{t['dataset']}.{t['table']}": {**safe_labels(t.get("dataset_labels")),
                                                                         **safe_labels(t.get("labels"))}
                    for t in tables_raw if t.get("dataset") and t.get("table")}
    by_dest = fb.add_transfers(raw.get("transfers") or [])
    fb.add_views(raw.get("bq_views") or [], table_labels)
    fb.add_bq_jobs(raw.get("bq_jobs") or [], raw.get("bq_job_details") or {}, by_dest)
    fb.add_dataflow(raw.get("dataflow_jobs") or [])
    cluster_costs = {c["name"]: c["sizing"]["current_monthly_usd"] for c in raw.get("clusters_sized") or []
                     if c.get("kind") == "dataproc" and c.get("sizing", {}).get("current_monthly_usd")}
    idle_by_cluster = fb.add_dataproc_jobs(raw.get("dataproc_jobs") or [], cluster_costs)
    fb.add_dags(raw.get("composer_dags") or [])
    fb.add_datastream(raw.get("datastream") or [])
    fb.add_services(raw.get("services") or [])
    fb.add_catalog(raw.get("catalog") or [])

    # capa de cada nodo de datos
    for nid, n in fb.data_nodes.items():
        if n.get("declared_layer"):
            n["layer"], n["layer_source"] = n["declared_layer"], "catálogo"
        elif n["system"] == "bigquery" and n.get("table"):
            t = tables.get(f"bq:{n['project']}.{n['dataset']}.{n['table']}")
            n["layer"], n["layer_source"] = (t["layer"], t["layer_source"]) if t else classify_layer(n["table"], n["dataset"])
        elif n["system"] == "gcs":
            b = lake_by_bucket.get(n.get("bucket", ""))
            layer, src = classify_layer(n["name"].split("/", 3)[-1] if "/" in n["name"][5:] else None, n.get("bucket"),
                                        None)
            n["layer"], n["layer_source"] = layer or (b or {}).get("layer"), src
        elif n["system"] in ("postgres", "mysql", "oracle", "sqlserver", "hdfs", "kafka", "http", "ext"):
            n["layer"], n["layer_source"] = "source", "sistema origen"
        else:
            n["layer"], n["layer_source"] = None, "n/d"

    flows = list(fb.flows.values())
    for f in flows:
        _finalize_flow(f, history_days)
        f["inputs"] = _io_list(f, "inputs", fb.data_nodes)
        f["outputs"] = _io_list(f, "outputs", fb.data_nodes)
        f["fingerprints"] = sorted(x for x in f["fingerprints"] if x)
        f["input_systems"] = sorted({i["system"] for i in f["inputs"] if i["system"]})
        f["layers_in"] = sorted({i["layer"] for i in f["inputs"] if i.get("layer")}, key=lambda x: LAYER_ORDER[x])
        f["layers_out"] = sorted({o["layer"] for o in f["outputs"] if o.get("layer")}, key=lambda x: LAYER_ORDER[x])
        for i in f["inputs"]:
            t = tables.get(i["id"].replace("data:bq:", "bq:"))
            if t:
                t["readers"].append(f["resource_id"])
        for o in f["outputs"]:
            t = tables.get(o["id"].replace("data:bq:", "bq:"))
            if t:
                t["writers"].append(f["resource_id"])
    for tid, t in tables.items():
        t["consumer_queries_30d"] = fb.table_consumers.get(tid.replace("bq:", "data:bq:"), 0)

    # Linaje: capas inferidas, fuente/destino crudos y grupos de cascada
    node_labels: dict[str, dict[str, str]] = {}
    ds_labels = {f"{d['project']}.{d['dataset']}": safe_labels(d.get("labels")) for d in raw.get("bq_datasets") or []
                 if d.get("dataset")}
    bucket_labels = {b["bucket"]: safe_labels(b.get("labels")) for b in raw.get("lake_buckets") or [] if b.get("bucket")}
    bucket_labels.update({k: safe_labels(v) for k, v in (raw.get("bucket_labels") or {}).items()})
    for nid, n in fb.data_nodes.items():
        if n["system"] == "bigquery" and n.get("table"):
            node_labels[nid] = {**ds_labels.get(f"{n['project']}.{n['dataset']}", {}),
                                **table_labels.get(f"bq:{n['project']}.{n['dataset']}.{n['table']}", {})}
        elif n["system"] == "gcs":
            node_labels[nid] = bucket_labels.get(n.get("bucket") or "", {})
        if node_labels.get(nid) and first_label(node_labels[nid], LAYER_KEYS):
            layer, src = infer_layer_from(None, None, node_labels[nid])
            if layer != NOT_DETECTED:
                n["layer"], n["layer_source"] = layer, src
                for f in flows:
                    for io in f["inputs"] + f["outputs"]:
                        if io["id"] == nid:
                            io["layer"] = layer
    for f in flows:
        enrich_flow_lineage(f, fb.data_nodes)
    dependencies, cascades = assign_cascades(flows, node_labels)

    issues = detect_issues(flows, fb.data_nodes, tables, fb.table_consumers)
    issues += lineage_issues(flows, cascades)
    for iss in issues:
        for fid in iss["flows"]:
            f = next((x for x in flows if x["resource_id"] == fid), None)
            if f is not None:
                f["signals"].append({"code": iss["code"], "severity": iss["severity"], "text": iss["text"]})

    lineage_edges = []
    for f in flows:
        for i in f["inputs"]:
            lineage_edges.append({"source": i["id"], "target": f["resource_id"], "kind": "reads"})
        for o in f["outputs"]:
            lineage_edges.append({"source": f["resource_id"], "target": o["id"], "kind": "writes",
                                  "mode": o.get("write_mode")})

    # desglose de costos: proyecto → flujos + compartidos
    shared = []
    for c in raw.get("clusters_sized") or []:
        s = c.get("sizing") or {}
        if c.get("kind") == "dataproc" and c["name"] in idle_by_cluster:
            shared.append({"resource": f"Dataproc {c['name']} (capacidad ociosa)", "monthly_usd": idle_by_cluster[c["name"]]})
        elif c.get("kind") in ("gke", "cloudsql", "composer"):
            shared.append({"resource": f"{c['kind'].upper()} {c['name']}", "monthly_usd": s.get("current_monthly_usd") or 0})
    bq_storage = round(sum(t["storage_monthly_usd"] for t in tables.values()), 2)
    if bq_storage:
        shared.append({"resource": "Almacenamiento BigQuery", "monthly_usd": bq_storage})
    gcs = round(sum(b["monthly_usd"] for b in lake), 2)
    if gcs:
        shared.append({"resource": "Almacenamiento Cloud Storage (data lake)", "monthly_usd": gcs})
    flows_cost = round(sum(f["cost_30d_usd"] for f in flows), 2)
    shared_cost = round(sum(s["monthly_usd"] or 0 for s in shared), 2)

    return {
        "project_id": pid, "history_days": history_days,
        "tables": sorted(tables.values(), key=lambda t: -t["size_gb"]),
        "datasets": datasets, "lake": lake,
        "flows": sorted(flows, key=lambda f: -f["cost_30d_usd"]),
        "lineage": {"nodes": list(fb.data_nodes.values()), "edges": lineage_edges},
        "issues": issues,
        "dependencies": dependencies,
        "cascades": cascades,
        "cost_breakdown": {"project_id": pid, "flows_total_usd": flows_cost, "shared": shared,
                           "shared_total_usd": shared_cost, "total_usd": round(flows_cost + shared_cost, 2),
                           "flows": [{"resource_id": f["resource_id"], "name": f["name"], "kind": f["kind"],
                                      "cost_30d_usd": f["cost_30d_usd"], "basis": f["cost_basis"]} for f in flows]},
        "summary": summarize(list(tables.values()), flows, lake, issues),
    }


def summarize(tables: list[dict[str, Any]], flows: list[dict[str, Any]], lake: list[dict[str, Any]],
              issues: list[dict[str, Any]]) -> dict[str, Any]:
    by_layer = Counter(t["layer"] or "sin capa" for t in tables)
    big = [t for t in tables if t["size_gb"] >= 1 and t["type"] == "BASE TABLE"]
    codes = Counter(i["code"] for i in issues)
    return {
        "tables": len(tables), "tables_by_layer": dict(by_layer),
        "pct_tables_with_layer": round(100 * (len(tables) - by_layer.get("sin capa", 0)) / len(tables), 1) if tables else None,
        "pct_large_tables_partitioned": round(100 * sum(t["partitioned"] for t in big) / len(big), 1) if big else None,
        "tables_with_pii": sum(1 for t in tables if t["pii_columns"]),
        "flows": len(flows), "flows_by_kind": dict(Counter(f["kind"] for f in flows)),
        "flows_by_state": dict(Counter(f["state"] for f in flows)),
        "cascades": len({f.get("grupo_cascada_id") for f in flows if f.get("grupo_cascada_id")}),
        "pct_flows_grouped_by_label": round(100 * sum(1 for f in flows if (f.get("origen_grupo") or "").startswith(
            ("etiqueta", "convención"))) / len(flows), 1) if flows else None,
        "buckets": len(lake), "issues_by_code": dict(codes),
        "flows_cost_30d_usd": round(sum(f["cost_30d_usd"] for f in flows), 2),
    }


def iter_resource_ids(dp: dict[str, Any]) -> Iterable[str]:
    for k in ("tables", "datasets", "lake", "flows"):
        for x in dp.get(k) or []:
            yield x["resource_id"]
