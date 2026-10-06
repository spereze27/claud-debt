"""Colectores de la PLATAFORMA DE DATOS de un proyecto GCP (sólo lectura).

  · BigQuery: datasets, tablas, columnas, almacenamiento (INFORMATION_SCHEMA) e historial
    de jobs (JOBS_BY_PROJECT) → linaje, modo de escritura, duración y costo por flujo.
  · BigQuery Data Transfer: consultas programadas y transferencias (GCS/PostgreSQL/…→BQ).
  · Dataflow: jobs, fuentes (job_metadata), destinos (transforms Write*), vCPU/memoria.
  · Dataproc: clusters (capacidad), jobs y batches (Spark/Hive, rutas hdfs:// y gs://).
  · Composer: lee el código de los DAGs del bucket del entorno y extrae entradas/salidas.
  · Datastream: CDC desde Postgres/MySQL/Oracle hacia BigQuery/GCS.
  · Cloud Storage: muestreo de objetos del data lake (formatos, tamaños, particionado).
  · Cloud Monitoring: uso de CPU/memoria de clusters, tráfico y tiempo facturable de servicios.

Cada sección es independiente: un permiso faltante sólo deja esa sección vacía.
Las funciones de parseo son puras (testeables sin red).
"""
from __future__ import annotations

import itertools
import logging
import re
import statistics
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

from backend.core import pricing as P
from backend.core.data_platform import normalize_ref, sql_io

logger = logging.getLogger(__name__)

MAX_BQ_JOBS = 20000
MAX_JOB_DETAILS = 80
MAX_DATAFLOW_DETAILS = 40
MAX_DAG_FILES = 80
MAX_BUCKETS_SAMPLED = 30
OBJECTS_PER_BUCKET = 1000
BQ_METADATA_MAX_BYTES = 20 * 1024 ** 3   # tope de seguridad para las consultas a INFORMATION_SCHEMA


# ======================================================================== parseo puro
def bq_region(location: str | None) -> str:
    return f"region-{(location or 'US').lower()}"


def parse_labels_option(value: str | None) -> dict[str, str]:
    """'[STRUCT("layer", "gold"), STRUCT("owner", "x")]' → {'layer': 'gold', 'owner': 'x'}"""
    return dict(re.findall(r'STRUCT\("([^"]+)",\s*"([^"]*)"\)', value or ""))


def _strip_quotes(v: str | None) -> str | None:
    if v is None:
        return None
    return v.strip().strip('"').strip("'") or None


INPUT_FLAGS = re.compile(r"^--?(in(put)?s?|src|source|from|read|origen|entrada)([_-].*)?$", re.I)
OUTPUT_FLAGS = re.compile(r"^--?(out(put)?s?|dest(ination)?|target|to|write|sink|salida|destino)([_-].*)?$", re.I)


def io_from_args(args: list[str]) -> tuple[list[str], list[str], list[str], str | None]:
    """Clasifica rutas/tablas en argumentos de un job Spark: (entradas, salidas, sin_dirección, modo)."""
    ins, outs, unknown, mode = [], [], [], None
    prev = ""
    for a in args or []:
        key, _, val = a.partition("=") if a.startswith("-") and "=" in a else ("", "", a)
        flag = key or prev
        if normalize_ref(val):
            (ins if INPUT_FLAGS.match(flag or "") else outs if OUTPUT_FLAGS.match(flag or "") else unknown).append(val)
        if re.search(r"overwrite", a, re.I):
            mode = "overwrite"
        elif re.search(r"\bappend\b", a, re.I):
            mode = mode or "append"
        prev = a if a.startswith("-") and "=" not in a else ""
    return ins, outs, unknown, mode


def _matching_paren(code: str, start: int) -> int:
    depth, i, quote = 0, start, None
    while i < len(code):
        ch = code[i]
        if quote:
            if code.startswith(quote, i):
                i += len(quote) - 1
                quote = None
            elif ch == "\\":
                i += 1
        elif code.startswith(('"""', "'''"), i):
            quote = code[i:i + 3]
            i += 2
        elif ch in "\"'":
            quote = ch
        elif ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                return i
        i += 1
    return len(code)


def _kw(block: str, *names: str) -> str | None:
    for n in names:
        m = re.search(rf"\b{n}\s*=\s*(?:f?)(\"\"\"|'''|\"|')(.*?)\1", block, re.S)
        if m:
            return m.group(2)
    return None


def _kw_list(block: str, name: str) -> list[str]:
    m = re.search(rf"\b{name}\s*=\s*\[([^\]]*)\]", block, re.S)
    if m:
        return re.findall(r"[\"']([^\"']+)[\"']", m.group(1))
    v = _kw(block, name)
    return [v] if v else []


def parse_dag_source(code: str, default_project: str | None) -> list[dict[str, Any]]:
    """Extrae DAGs de un archivo Python de Airflow: id, schedule, operadores, entradas y salidas."""
    ids = re.findall(r"\bDAG\(\s*(?:dag_id\s*=\s*)?[\"']([^\"']+)[\"']", code)
    ids += re.findall(r"\bdag_id\s*=\s*[\"']([^\"']+)[\"']", code)
    ids += re.findall(r"@dag\([^)]*?dag_id\s*=\s*[\"']([^\"']+)", code, re.S)
    if not ids:
        m = re.search(r"@dag\b[^\n]*\n\s*def\s+(\w+)", code)
        ids = [m.group(1)] if m else []
    ids = list(dict.fromkeys(ids))
    if not ids:
        return []
    sched = re.search(r"\bschedule(?:_interval)?\s*=\s*([^,\n)]+)", code)
    schedule = _strip_quotes(sched.group(1)) if sched else None

    inputs: set[str] = set()
    outputs: dict[str, str] = {}
    operators: list[str] = []
    systems: set[str] = set()
    for m in re.finditer(r"\b(\w+Operator|\w+Sensor)\s*\(", code):
        op = m.group(1)
        operators.append(op)
        block = code[m.end() - 1:_matching_paren(code, m.end() - 1) + 1]
        wd = (_kw(block, "write_disposition", "writeDisposition") or "").upper()
        mode = {"WRITE_TRUNCATE": "overwrite", "WRITE_APPEND": "append", "WRITE_EMPTY": "create"}.get(wd)
        dest = _kw(block, "destination_project_dataset_table", "destination_dataset_table")
        if not dest:
            dm = re.search(r"[\"']destinationTable[\"']\s*:\s*\{([^}]*)\}", block)
            if dm:
                parts = dict(re.findall(r"[\"'](projectId|datasetId|tableId)[\"']\s*:\s*[\"']([^\"']+)", dm.group(1)))
                if parts.get("datasetId") and parts.get("tableId"):
                    dest = f"{parts.get('projectId', default_project)}.{parts['datasetId']}.{parts['tableId']}"
        if dest:
            outputs[dest] = mode or ("create" if "GCSToBigQuery" in op else "unknown")
        bucket = _kw(block, "bucket", "source_bucket")
        for obj in _kw_list(block, "source_objects") + _kw_list(block, "source_object"):
            if bucket:
                inputs.add(f"gs://{bucket}/{obj}")
        if re.search(r"(ToGCS|ToGcs)Operator", op):
            fname = _kw(block, "filename", "object_name")
            if bucket and fname:
                outputs[f"gs://{bucket}/{fname}"] = "overwrite"
        for sysname, kw in (("postgres", "postgres_conn_id"), ("mysql", "mysql_conn_id"), ("mssql", "mssql_conn_id"),
                            ("oracle", "oracle_conn_id"), ("hdfs", "hdfs_conn_id"), ("sftp", "sftp_conn_id")):
            conn = _kw(block, kw)
            if conn:
                systems.add(sysname)
                inputs.add(f"{'postgresql' if sysname == 'postgres' else sysname}://{conn}/")
        dst_bucket = _kw(block, "destination_bucket")
        if dst_bucket:
            outputs[f"gs://{dst_bucket}/{_kw(block, 'destination_object') or ''}"] = "overwrite"
        for sql in re.findall(r"(\"\"\"|'''|\"|')((?:(?!\1).)*?(?:SELECT|INSERT|MERGE|CREATE)(?:(?!\1).)*)\1", block, re.S | re.I):
            reads, writes = sql_io(sql[1], default_project)
            inputs.update(reads)
            for w, wmode in writes:
                outputs.setdefault(w, wmode)
        for uri in re.findall(r"[\"']((?:gs|hdfs)://[^\"'{}\s]+)[\"']", block):
            if uri not in outputs:
                inputs.add(uri)
    return [{"dag_id": d, "schedule": schedule, "operators": operators, "inputs": sorted(inputs - set(outputs)),
             "outputs": outputs, "systems": sorted(systems)} for d in ids]


def dataflow_io(job_dict: dict[str, Any]) -> tuple[list[str], list[str]]:
    """Entradas: job_metadata (describe las FUENTES). Salidas: transforms cuyo nombre contiene Write/Sink."""
    ins: list[str] = []
    md = job_dict.get("jobMetadata") or {}
    for d in md.get("bigqueryDetails") or []:
        if d.get("table"):
            ins.append(f"{d.get('projectId')}.{d.get('dataset')}.{d['table']}")
    for d in md.get("pubsubDetails") or []:
        if d.get("subscription"):
            ins.append(d["subscription"] if d["subscription"].startswith("projects/") else f"projects/_/subscriptions/{d['subscription']}")
        elif d.get("topic"):
            ins.append(d["topic"] if d["topic"].startswith("projects/") else f"projects/_/topics/{d['topic']}")
    for d in md.get("fileDetails") or []:
        if d.get("filePattern"):
            ins.append(d["filePattern"])
    outs: list[str] = []
    for t in (job_dict.get("pipelineDescription") or {}).get("originalPipelineTransform") or []:
        name = (t.get("name") or "").lower()
        if not re.search(r"write|sink|output|insert|publish", name):
            continue
        for dd in t.get("displayData") or []:
            v = dd.get("strValue") or ""
            if v and normalize_ref(v.replace(":", ".", 1) if re.match(r"^[\w\-]+:[\w]+\.[\w]+$", v) else v):
                outs.append(v.replace(":", ".", 1) if re.match(r"^[\w\-]+:[\w]+\.[\w]+$", v) else v)
    return list(dict.fromkeys(ins)), list(dict.fromkeys(outs))


def summarize_objects(objects: list[tuple[str, int, datetime | None]], now: datetime) -> dict[str, Any]:
    """objects = [(nombre, bytes, creado)] → formatos, tamaños, prefijos, particionado, antigüedad."""
    ext_counts: Counter = Counter()
    ext_bytes: Counter = Counter()
    prefixes: Counter = Counter()
    ages, sizes = [], []
    partition_hits = 0
    for name, size, created in objects:
        if name.endswith("/"):
            continue
        m = re.search(r"\.([a-z0-9]{2,8})(?:\.(?:gz|snappy|zst|bz2|zip))?$", name.lower())
        ext = m.group(1) if m else "(sin extensión)"
        ext_counts[ext] += 1
        ext_bytes[ext] += size
        sizes.append(size)
        prefixes[name.split("/")[0] if "/" in name else "(raíz)"] += 1
        if re.search(r"/[\w\-]+=[^/]+/|/\d{4}/\d{2}/\d{2}/|/\d{8}/", "/" + name):
            partition_hits += 1
        if created:
            ages.append((now - created).days)
    n = len(sizes)
    return {
        "sampled_objects": n, "sampled_bytes": sum(sizes), "ext_counts": dict(ext_counts), "ext_bytes": dict(ext_bytes),
        "avg_object_mb": round(statistics.mean(sizes) / 1024 ** 2, 2) if sizes else None,
        "small_files_pct": round(100 * sum(1 for s in sizes if s < 32 * 1024 ** 2) / n, 1) if n else None,
        "top_prefixes": [p for p, _ in prefixes.most_common(15)],
        "partition_style": partition_hits >= max(1, n * 0.3) if n else None,
        "median_age_days": statistics.median(ages) if ages else None,
    }


def util_stats(values: list[float]) -> dict[str, Any] | None:
    vals = sorted(v * 100 if v <= 1.5 else v for v in values if v is not None)
    if len(vals) < 12:
        return None
    return {"p95": round(vals[max(0, int(len(vals) * 0.95) - 1)], 1), "avg": round(statistics.mean(vals), 1),
            "active_hours_pct": round(100 * sum(1 for v in vals if v > 10) / len(vals), 1), "samples": len(vals)}


# ======================================================================== métricas
class Metrics:
    def __init__(self, credentials: Any, project_id: str) -> None:
        from google.cloud import monitoring_v3

        self.m = monitoring_v3
        self.client = monitoring_v3.MetricServiceClient(credentials=credentials)
        self.project = project_id
        self.now = datetime.now(timezone.utc)

    @staticmethod
    def _group_value(series: Any, field: str) -> str | None:
        key = field.split(".")[-1].strip('"')
        for labels in (series.resource.labels, series.metric.labels, getattr(series.metadata, "user_labels", None)):
            if labels and key in labels:
                return labels[key]
        return None

    def _query(self, metric: str, group_by: str, days: int, period_s: int, aligner: Any, reducer: Any,
               extra_filter: str = "") -> dict[str, list[float]]:
        req = {
            "name": f"projects/{self.project}",
            "filter": f'metric.type = "{metric}"' + (f" AND {extra_filter}" if extra_filter else ""),
            "interval": self.m.TimeInterval(start_time=self.now - timedelta(days=days), end_time=self.now),
            "aggregation": self.m.Aggregation(alignment_period={"seconds": period_s}, per_series_aligner=aligner,
                                              cross_series_reducer=reducer, group_by_fields=[group_by]),
            "view": self.m.ListTimeSeriesRequest.TimeSeriesView.FULL,
        }
        out: dict[str, list[float]] = {}
        for s in self.client.list_time_series(request=req, timeout=60):
            key = self._group_value(s, group_by)
            if key is None:
                continue
            out.setdefault(key, []).extend(
                p.value.double_value if p.value.double_value else float(p.value.int64_value) for p in s.points)
        return out

    def hourly(self, metric: str, group_by: str, days: int = 14, extra_filter: str = "") -> dict[str, list[float]]:
        A = self.m.Aggregation
        return self._query(metric, group_by, days, 3600, A.Aligner.ALIGN_MEAN, A.Reducer.REDUCE_MEAN, extra_filter)

    def total(self, metric: str, group_by: str, days: int = 30, extra_filter: str = "") -> dict[str, float]:
        A = self.m.Aggregation
        raw = self._query(metric, group_by, days, days * 86400, A.Aligner.ALIGN_DELTA, A.Reducer.REDUCE_SUM, extra_filter)
        return {k: float(sum(v)) for k, v in raw.items()}

    def latest(self, metric: str, group_by: str, extra_filter: str = "") -> dict[str, float]:
        A = self.m.Aggregation
        raw = self._query(metric, group_by, 2, 86400, A.Aligner.ALIGN_MEAN, A.Reducer.REDUCE_SUM, extra_filter)
        return {k: float(v[0]) for k, v in raw.items() if v}


# ======================================================================== colector
class GCPDataCollector:
    def __init__(self, credentials: Any, project_id: str, assets: list[dict[str, Any]], arch: dict[str, Any],
                 history_days: int = 30, sample_lake: bool = True, read_dags: bool = True,
                 known_buckets: list[str] | None = None) -> None:
        self.credentials = credentials
        self.project_id = project_id
        self.assets = assets or []
        self.arch = arch or {}
        self.days = max(1, min(int(history_days or 30), 180))
        self.sample_lake = sample_lake
        self.read_dags = read_dags
        self.known_buckets = known_buckets or []
        self.now = datetime.now(timezone.utc)
        self.self_email = getattr(credentials, "service_account_email", None)
        self.enabled = set(self.arch.get("enabled_apis") or [])
        self.errors: list[str] = []
        self.ok: list[str] = []
        self._metrics: Metrics | None = None

    # ---------- utilidades
    def _api_on(self, api: str) -> bool:
        return not self.enabled or api in self.enabled

    def _assets(self, asset_type: str) -> list[dict[str, Any]]:
        return [a for a in self.assets if a.get("asset_type") == asset_type]

    def _section(self, name: str, fn: Callable[[], Any], describe: Callable[[str, Exception], str], default: Any) -> Any:
        try:
            out = fn()
            self.ok.append(name)
            return out
        except Exception as exc:  # noqa: BLE001
            logger.warning("Colector %s falló: %s", name, exc)
            self.errors.append(describe(name, exc))
            return default

    @property
    def metrics(self) -> Metrics:
        if self._metrics is None:
            self._metrics = Metrics(self.credentials, self.project_id)
        return self._metrics

    def _bq(self) -> Any:
        from google.cloud import bigquery

        return bigquery.Client(project=self.project_id, credentials=self.credentials)

    def _run_sql(self, client: Any, sql: str, location: str, params: list[Any] | None = None) -> list[dict[str, Any]]:
        from google.cloud import bigquery

        cfg = bigquery.QueryJobConfig(maximum_bytes_billed=BQ_METADATA_MAX_BYTES, query_parameters=params or [],
                                      labels={"tool": "cloud-tech-debt-analyzer"})
        return [dict(r) for r in client.query(sql, job_config=cfg, location=location).result(timeout=120)]

    # ---------- BigQuery: metadatos
    def bigquery_metadata(self) -> dict[str, Any]:
        client = self._bq()
        datasets, locations = [], set()
        for item in itertools.islice(client.list_datasets(), 500):
            ds = client.get_dataset(item.reference, timeout=30)
            locations.add(ds.location or "US")
            datasets.append({"project": self.project_id, "dataset": ds.dataset_id, "location": ds.location,
                             "labels": dict(ds.labels or {}), "description": ds.description,
                             "storage_billing_model": getattr(ds, "storage_billing_model", None)})
        ds_labels = {d["dataset"]: d["labels"] for d in datasets}
        tables: dict[tuple[str, str], dict[str, Any]] = {}
        views: list[dict[str, Any]] = []
        p = self.project_id
        for loc in sorted(locations):
            reg = bq_region(loc)
            for r in self._run_sql(client, f"SELECT table_schema, table_name, table_type, creation_time "
                                           f"FROM `{p}`.`{reg}`.INFORMATION_SCHEMA.TABLES", loc):
                tables[(r["table_schema"], r["table_name"])] = {
                    "project": p, "dataset": r["table_schema"], "table": r["table_name"], "type": r["table_type"],
                    "created": r["creation_time"].isoformat() if r.get("creation_time") else None, "columns": [],
                    "labels": {}, "dataset_labels": ds_labels.get(r["table_schema"], {}), "location": loc}
            for r in self._run_sql(client, f"SELECT table_schema, table_name, option_name, option_value FROM "
                                           f"`{p}`.`{reg}`.INFORMATION_SCHEMA.TABLE_OPTIONS "
                                           f"WHERE option_name IN ('description', 'labels')", loc):
                t = tables.get((r["table_schema"], r["table_name"]))
                if t:
                    if r["option_name"] == "labels":
                        t["labels"] = parse_labels_option(r["option_value"])
                    else:
                        t["description"] = _strip_quotes(r["option_value"])
            try:
                for r in self._run_sql(client, f"SELECT table_schema, table_name, total_rows, total_logical_bytes, "
                                               f"active_logical_bytes, long_term_logical_bytes, active_physical_bytes, "
                                               f"long_term_physical_bytes, storage_last_modified_time "
                                               f"FROM `{p}`.`{reg}`.INFORMATION_SCHEMA.TABLE_STORAGE "
                                               f"WHERE project_id = '{p}'", loc):
                    t = tables.get((r["table_schema"], r["table_name"]))
                    if t:
                        t.update(rows=r["total_rows"], logical_bytes=r["total_logical_bytes"],
                                 active_logical=r["active_logical_bytes"], lt_logical=r["long_term_logical_bytes"],
                                 active_physical=r["active_physical_bytes"], lt_physical=r["long_term_physical_bytes"],
                                 last_modified=r["storage_last_modified_time"].isoformat()
                                 if r.get("storage_last_modified_time") else None)
            except Exception as exc:  # noqa: BLE001 — TABLE_STORAGE requiere bigquery.tables.list a nivel proyecto
                self.errors.append(f"AVISO en 'bigquery_storage' ({loc}): {str(exc)[:200]}")
            try:
                for r in self._run_sql(client, f"SELECT table_schema, table_name, view_definition "
                                               f"FROM `{p}`.`{reg}`.INFORMATION_SCHEMA.VIEWS", loc):
                    views.append({"project": p, "dataset": r.get("table_schema"), "view": r.get("table_name"),
                                  "definition": r.get("view_definition")})
            except Exception as exc:  # noqa: BLE001
                self.errors.append(f"AVISO en 'bigquery_views' ({loc}): {str(exc)[:200]}")
            for r in self._run_sql(client, f"SELECT table_schema, table_name, column_name, data_type, "
                                           f"is_partitioning_column, clustering_ordinal_position "
                                           f"FROM `{p}`.`{reg}`.INFORMATION_SCHEMA.COLUMNS "
                                           f"ORDER BY table_schema, table_name, ordinal_position LIMIT 200000", loc):
                t = tables.get((r["table_schema"], r["table_name"]))
                if not t:
                    continue
                dt = r["data_type"] or ""
                t["columns"].append({"name": r["column_name"], "type": dt.split("<")[0],
                                     "mode": "REPEATED" if dt.startswith("ARRAY") else "NULLABLE"})
                if r.get("is_partitioning_column") == "YES":
                    t["partition_column"] = r["column_name"]
                if r.get("clustering_ordinal_position"):
                    t.setdefault("clustering", []).append(r["column_name"])
        return {"bq_datasets": datasets, "bq_tables": list(tables.values()), "bq_views": views,
                "bq_locations": sorted(locations)}

    # ---------- BigQuery: historial de jobs
    def bigquery_jobs(self, locations: list[str]) -> dict[str, Any]:
        from google.cloud import bigquery

        client = self._bq()
        jobs: list[dict[str, Any]] = []
        p = self.project_id
        for loc in locations or ["US"]:
            sql = f"""
                SELECT job_id, parent_job_id, creation_time, end_time, user_email, job_type, statement_type, state,
                       error_result.reason AS error_reason,
                       IF(destination_table.table_id IS NULL, NULL, CONCAT(destination_table.project_id, '.',
                          destination_table.dataset_id, '.', destination_table.table_id)) AS destination,
                       ARRAY(SELECT CONCAT(r.project_id, '.', r.dataset_id, '.', r.table_id)
                             FROM UNNEST(referenced_tables) r) AS referenced,
                       total_bytes_billed AS bytes_billed, total_slot_ms AS slot_ms, cache_hit,
                       ARRAY(SELECT AS STRUCT l.key, l.value FROM UNNEST(labels) l) AS labels,
                       SUBSTR(query, 0, 1500) AS query
                FROM `{p}`.`{bq_region(loc)}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
                WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @days DAY)
                  AND job_type IN ('QUERY', 'LOAD', 'COPY')
                  AND IFNULL(statement_type, '') != 'SCRIPT'
                ORDER BY creation_time DESC LIMIT {MAX_BQ_JOBS}"""
            for r in self._run_sql(client, sql, loc, [bigquery.ScalarQueryParameter("days", "INT64", self.days)]):
                r["labels"] = {x.get("key"): x.get("value") for x in r.get("labels") or [] if x and x.get("key")}
                r["creation_time"] = r["creation_time"].isoformat() if r.get("creation_time") else None
                r["end_time"] = r["end_time"].isoformat() if r.get("end_time") else None
                r["location"] = loc
                jobs.append(r)
        # Detalle (write_disposition, source_uris) de un job representativo por (identidad, destino)
        details: dict[str, dict[str, Any]] = {}
        seen: set[tuple] = set()
        for j in jobs:
            stmt = (j.get("statement_type") or "").upper()
            if not j.get("destination") or j["destination"].split(".")[1].startswith("_"):
                continue
            if stmt in ("INSERT", "MERGE", "UPDATE", "DELETE", "CREATE_TABLE_AS_SELECT", "TRUNCATE_TABLE"):
                continue
            key = (j.get("user_email"), j["destination"], j.get("job_type"))
            if key in seen or len(details) >= MAX_JOB_DETAILS:
                continue
            seen.add(key)
            try:
                job = client.get_job(j["job_id"], location=j["location"], timeout=20)
                details[j["job_id"]] = {"write_disposition": getattr(job, "write_disposition", None),
                                        "source_uris": list(getattr(job, "source_uris", None) or [])[:20],
                                        "source_format": getattr(job, "source_format", None)}
            except Exception:  # noqa: BLE001
                continue
        return {"bq_jobs": jobs, "bq_job_details": details}

    # ---------- Data Transfer
    def transfers(self, locations: list[str]) -> list[dict[str, Any]]:
        from google.cloud import bigquery_datatransfer_v1 as dts
        from google.protobuf import json_format

        client = dts.DataTransferServiceClient(credentials=self.credentials)
        out, seen = [], set()
        for loc in sorted({(l or "us").lower() for l in (locations or ["us"])}):
            for cfg in client.list_transfer_configs(parent=f"projects/{self.project_id}/locations/{loc}", timeout=30):
                if cfg.name in seen:
                    continue
                seen.add(cfg.name)
                runs = []
                for run in itertools.islice(client.list_transfer_runs(parent=cfg.name, timeout=30), 15):
                    runs.append({"state": run.state.name, "start": run.start_time.isoformat() if run.start_time else None,
                                 "end": run.end_time.isoformat() if run.end_time else None,
                                 "error": run.error_status.message or None})
                out.append({"name": cfg.name, "display_name": cfg.display_name, "data_source_id": cfg.data_source_id,
                            "destination_dataset": cfg.destination_dataset_id, "schedule": cfg.schedule,
                            "disabled": cfg.disabled, "state": cfg.state.name,
                            "params": json_format.MessageToDict(dts.TransferConfig.pb(cfg).params), "runs": runs,
                            "location": loc})
        return out

    # ---------- Dataflow
    def dataflow(self) -> list[dict[str, Any]]:
        from google.cloud import dataflow_v1beta3 as df
        from google.protobuf import json_format

        jobs_client = df.JobsV1Beta3Client(credentials=self.credentials)
        metrics_client = df.MetricsV1Beta3Client(credentials=self.credentials)
        since = self.now - timedelta(days=self.days)
        jobs = []
        req = df.ListJobsRequest(project_id=self.project_id, filter=df.ListJobsRequest.Filter.ALL,
                                 view=df.JobView.JOB_VIEW_SUMMARY)
        for j in itertools.islice(jobs_client.aggregated_list_jobs(request=req, timeout=60), 500):
            created = j.create_time
            running = j.current_state.name == "JOB_STATE_RUNNING"
            if created and created < since and not running:
                continue
            jobs.append(j)
        out, detailed = [], set()
        for j in sorted(jobs, key=lambda x: x.create_time, reverse=True):
            base = re.sub(r"[-_]?\d{6,}.*$|[-_][0-9a-f]{8,}$", "", j.name) or j.name
            item = {"id": j.id, "name": j.name, "type": j.type_.name, "state": j.current_state.name,
                    "location": j.location, "create_time": j.create_time.isoformat() if j.create_time else None,
                    "start_time": j.start_time.isoformat() if j.start_time else None,
                    "end_time": j.current_state_time.isoformat() if j.current_state_time else None,
                    "labels": dict(j.labels or {}), "inputs": [], "outputs": []}
            if base not in detailed and len(detailed) < MAX_DATAFLOW_DETAILS:
                detailed.add(base)
                try:
                    full = jobs_client.get_job(request=df.GetJobRequest(project_id=self.project_id, job_id=j.id,
                                                                        location=j.location, view=df.JobView.JOB_VIEW_ALL),
                                               timeout=30)
                    d = json_format.MessageToDict(df.Job.pb(full))
                    item["inputs"], item["outputs"] = dataflow_io(d)
                    env = d.get("environment") or {}
                    item["service_account"] = env.get("serviceAccountEmail")
                    pools = env.get("workerPools") or []
                    if pools:
                        item["worker_machine"] = pools[0].get("machineType")
                        item["max_workers"] = (pools[0].get("autoscalingSettings") or {}).get("maxNumWorkers")
                    m = metrics_client.get_job_metrics(request=df.GetJobMetricsRequest(
                        project_id=self.project_id, job_id=j.id, location=j.location), timeout=30)
                    for mu in m.metrics:
                        if mu.name.origin != "dataflow/v1b3" or dict(mu.name.context).get("tentative") == "true":
                            continue
                        val = float(mu.scalar) if mu.scalar is not None else 0.0
                        if mu.name.name == "TotalVcpuTime":
                            item["vcpu_s"] = val
                        elif mu.name.name == "TotalMemoryUsage":
                            item["mem_gb_s"] = val / 1024
                    if j.start_time:
                        end = j.current_state_time if item["state"] != "JOB_STATE_RUNNING" else self.now
                        item["elapsed_s"] = (end - j.start_time).total_seconds()
                except Exception as exc:  # noqa: BLE001
                    logger.info("Detalle Dataflow %s: %s", j.id, exc)
            out.append(item)
        return out

    # ---------- Dataproc
    def _regions(self) -> list[str]:
        regs = {a.get("location") for a in self.assets if a.get("asset_type", "").startswith("dataproc.")}
        regs |= {re.sub(r"-[a-z]$", "", a.get("location") or "") for a in self.assets
                 if a.get("asset_type") in ("compute.googleapis.com/Instance", "run.googleapis.com/Service")}
        regs = {r for r in regs if r and r not in ("global", "us", "eu")}
        return sorted(regs or {"us-central1"})[:8]

    def dataproc(self) -> dict[str, Any]:
        from google.cloud import dataproc_v1 as dp

        clusters, jobs = [], []
        since = self.now - timedelta(days=self.days)
        for region in self._regions():
            opts = {"api_endpoint": f"{region}-dataproc.googleapis.com:443"}
            cc = dp.ClusterControllerClient(credentials=self.credentials, client_options=opts)
            jc = dp.JobControllerClient(credentials=self.credentials, client_options=opts)
            bc = dp.BatchControllerClient(credentials=self.credentials, client_options=opts)
            for c in cc.list_clusters(project_id=self.project_id, region=region, timeout=30):
                clusters.append(self._dataproc_cluster(c, region))
            for j in itertools.islice(jc.list_jobs(project_id=self.project_id, region=region, timeout=30), 400):
                start = j.status_history[0].state_start_time if j.status_history else j.status.state_start_time
                if start and start < since:
                    continue
                jobs.append(self._dataproc_job(j, region))
            try:
                for b in itertools.islice(bc.list_batches(parent=f"projects/{self.project_id}/locations/{region}",
                                                          timeout=30), 200):
                    if b.create_time and b.create_time < since:
                        continue
                    jobs.append(self._dataproc_batch(b, region))
            except Exception as exc:  # noqa: BLE001
                logger.info("Batches Dataproc %s: %s", region, exc)
        return {"clusters": clusters, "jobs": jobs}

    @staticmethod
    def _dataproc_cluster(c: Any, region: str) -> dict[str, Any]:
        cfg = c.config
        groups = []
        for role, g, scalable in (("master", cfg.master_config, False), ("worker", cfg.worker_config, True),
                                  ("secondary_worker", cfg.secondary_worker_config, True)):
            if g and g.num_instances:
                groups.append({"role": role, "machine_type": (g.machine_type_uri or "").rsplit("/", 1)[-1],
                               "count": g.num_instances, "preemptible": bool(g.is_preemptible) or role == "secondary_worker",
                               "disk_gb": g.disk_config.boot_disk_size_gb, "disk_type": g.disk_config.boot_disk_type or "pd-standard",
                               "local_ssds": g.disk_config.num_local_ssds, "scalable": scalable})
        ttl = cfg.lifecycle_config.idle_delete_ttl
        worker = next((g for g in groups if g["role"] == "worker"), {})
        return {"resource_id": f"cluster:dataproc:{c.cluster_name}", "kind": "dataproc", "name": c.cluster_name,
                "location": region, "state": c.status.state.name, "node_groups": groups,
                "autoscaling": bool(cfg.autoscaling_config.policy_uri),
                "idle_delete_ttl_s": ttl.total_seconds() if ttl else None,
                "image_version": cfg.software_config.image_version, "labels": dict(c.labels or {}),
                "hdfs_heavy": (worker.get("disk_gb") or 0) >= 500 or (worker.get("local_ssds") or 0) > 0,
                "environment": (c.labels or {}).get("env") or (c.labels or {}).get("environment")}

    @staticmethod
    def _dataproc_job(j: Any, region: str) -> dict[str, Any]:
        jtype, main, args, sql = "job", None, [], ""
        from google.cloud import dataproc_v1 as dp

        pb = dp.Job.pb(j)
        for t in ("spark_job", "pyspark_job", "hive_job", "spark_sql_job", "hadoop_job", "presto_job", "trino_job"):
            if pb.HasField(t):
                spec = getattr(j, t)
                jtype = t.replace("_job", "")
                main = getattr(spec, "main_python_file_uri", None) or getattr(spec, "main_class", None) \
                    or getattr(spec, "main_jar_file_uri", None) or getattr(spec, "query_file_uri", None)
                args = list(getattr(spec, "args", []) or [])
                ql = getattr(spec, "query_list", None)
                sql = " ".join(ql.queries) if ql else ""
                break
        ins, outs, unk, mode = io_from_args(args)
        if sql:
            reads, writes = sql_io(sql, None)
            ins += reads
            outs += [w for w, _ in writes]
        hist = list(j.status_history)
        start = hist[0].state_start_time if hist else j.status.state_start_time
        end = j.status.state_start_time if j.done else None
        return {"id": j.reference.job_id, "cluster": j.placement.cluster_name, "region": region, "type": jtype,
                "main": main, "args": args[:20], "state": j.status.state.name,
                "start": start.isoformat() if start else None, "end": end.isoformat() if end else None,
                "inputs": ins, "outputs": outs, "io": unk, "write_mode": mode, "labels": dict(j.labels or {})}

    @staticmethod
    def _dataproc_batch(b: Any, region: str) -> dict[str, Any]:
        from google.cloud import dataproc_v1 as dp

        main, args = None, []
        pb = dp.Batch.pb(b)
        for t in ("pyspark_batch", "spark_batch", "spark_sql_batch"):
            if pb.HasField(t):
                spec = getattr(b, t)
                main = getattr(spec, "main_python_file_uri", None) or getattr(spec, "main_class", None) \
                    or getattr(spec, "query_file_uri", None)
                args = list(getattr(spec, "args", []) or [])
                break
        ins, outs, unk, mode = io_from_args(args)
        usage = b.runtime_info.approximate_usage if b.runtime_info else None
        return {"id": b.name.rsplit("/", 1)[-1], "cluster": None, "region": region, "type": "serverless",
                "main": main, "args": args[:20], "state": b.state.name,
                "start": b.create_time.isoformat() if b.create_time else None,
                "end": b.state_time.isoformat() if b.state_time and b.state.name in ("SUCCEEDED", "FAILED", "CANCELLED") else None,
                "inputs": ins, "outputs": outs, "io": unk, "write_mode": mode,
                "dcu_hours": (usage.milli_dcu_seconds / 1000 / 3600) if usage else None}

    # ---------- Composer
    def composer(self, describe: Callable[[str, Exception], str]) -> dict[str, Any]:
        from google.cloud import storage

        envs, dags = [], []
        client = storage.Client(project=self.project_id, credentials=self.credentials)
        for a in self._assets("composer.googleapis.com/Environment"):
            d = a.get("data") or {}
            cfg = d.get("config") or {}
            name = (d.get("name") or a["name"]).rsplit("/", 1)[-1]
            prefix = cfg.get("dagGcsPrefix")
            env = {"resource_id": f"cluster:composer:{name}", "kind": "composer", "name": name,
                   "location": a.get("location"), "environment_size": cfg.get("environmentSize"),
                   "node_count": cfg.get("nodeCount"), "image": (cfg.get("softwareConfig") or {}).get("imageVersion"),
                   "dag_prefix": prefix, "dag_count": None, "state": d.get("state")}
            if prefix and self.read_dags:
                try:
                    bucket, _, path = prefix[5:].partition("/")
                    files = [b for b in itertools.islice(client.list_blobs(bucket, prefix=(path or "dags") + "/",
                                                                           timeout=30), 2000)
                             if b.name.endswith(".py") and (b.size or 0) < 300_000][:MAX_DAG_FILES]
                    for blob in files:
                        code = blob.download_as_text(timeout=30)
                        for dag in parse_dag_source(code, self.project_id):
                            dags.append({**dag, "environment": name, "file": blob.name})
                    env["dag_count"] = len({x["dag_id"] for x in dags if x["environment"] == name})
                except Exception as exc:  # noqa: BLE001
                    self.errors.append(describe("composer_dags", exc))
            envs.append(env)
        return {"environments": envs, "dags": dags}

    # ---------- Datastream (desde assets)
    def datastream(self) -> list[dict[str, Any]]:
        profiles = {}
        for a in self._assets("datastream.googleapis.com/ConnectionProfile"):
            d = a.get("data") or {}
            for kind in ("postgresqlProfile", "mysqlProfile", "oracleProfile", "sqlServerProfile"):
                if d.get(kind):
                    eng = kind.replace("Profile", "").replace("postgresql", "postgresql").lower()
                    profiles[d.get("name") or a["name"]] = f"{eng}://{d[kind].get('hostname')}/{d[kind].get('database', '')}"
            if d.get("gcsProfile"):
                profiles[d.get("name") or a["name"]] = f"gs://{d['gcsProfile'].get('bucket')}/{d['gcsProfile'].get('rootPath', '').strip('/')}"
        out = []
        for a in self._assets("datastream.googleapis.com/Stream"):
            d = a.get("data") or {}
            src_prof = (d.get("sourceConfig") or {}).get("sourceConnectionProfile")
            dst = d.get("destinationConfig") or {}
            dest = None
            bqd = dst.get("bigqueryDestinationConfig") or {}
            if bqd.get("singleTargetDataset"):
                ds = bqd["singleTargetDataset"].get("datasetId", "").replace(":", ".")
                dest = {"id": f"data:bqds:{ds}", "system": "bigquery", "name": f"{ds} (dataset)"}
            elif bqd.get("sourceHierarchyDatasets"):
                tmpl = (bqd["sourceHierarchyDatasets"].get("datasetTemplate") or {})
                dest = {"id": f"data:bqds:{self.project_id}.{tmpl.get('datasetIdPrefix', '')}*",
                        "system": "bigquery", "name": f"{tmpl.get('datasetIdPrefix', '')}<esquema> (datasets por esquema)"}
            elif dst.get("gcsDestinationConfig"):
                base = profiles.get(dst.get("destinationConnectionProfile"), "gs://?")
                dest = f"{base}/{dst['gcsDestinationConfig'].get('path', '').strip('/')}"
            out.append({"name": (d.get("displayName") or a["name"].rsplit("/", 1)[-1]), "state": d.get("state"),
                        "source": profiles.get(src_prof, src_prof), "destination": dest,
                        "mode": "merge" if "merge" in bqd else "append"})
        return out

    # ---------- Data lake (GCS)
    def lake(self) -> list[dict[str, Any]]:
        from google.cloud import storage

        client = storage.Client(project=self.project_id, credentials=self.credentials)
        names = list(dict.fromkeys(self.known_buckets))[:MAX_BUCKETS_SAMPLED]
        sizes, reqs = {}, {}
        try:
            sizes = self.metrics.latest("storage.googleapis.com/storage/total_bytes", "resource.label.bucket_name")
            reqs = self.metrics.total("storage.googleapis.com/api/request_count", "resource.label.bucket_name", 30)
        except Exception as exc:  # noqa: BLE001
            logger.info("Métricas de GCS: %s", exc)
        out = []
        for name in names:
            try:
                b = client.get_bucket(name, timeout=30)
                objs = []
                if self.sample_lake:
                    for blob in itertools.islice(client.list_blobs(name, timeout=60, fields="items(name,size,timeCreated),nextPageToken"),
                                                 OBJECTS_PER_BUCKET):
                        objs.append((blob.name, int(blob.size or 0), blob.time_created))
                summary = summarize_objects(objs, self.now)
                out.append({"bucket": name, "project_id": self.project_id, "storage_class": b.storage_class,
                            "location": b.location, "labels": dict(b.labels or {}),
                            "lifecycle_rules": len(list(b.lifecycle_rules or [])), "versioning": b.versioning_enabled,
                            "size_bytes": sizes.get(name) or summary["sampled_bytes"], "requests_30d": reqs.get(name),
                            **summary})
            except Exception as exc:  # noqa: BLE001
                logger.info("Bucket %s: %s", name, exc)
        return out

    # ---------- Capacidad (GKE, Cloud SQL) + uso
    def capacity(self, dataproc_clusters: list[dict[str, Any]], composer_envs: list[dict[str, Any]]) -> list[dict[str, Any]]:
        clusters = list(dataproc_clusters) + list(composer_envs)
        for a in self._assets("container.googleapis.com/Cluster"):
            d = a.get("data") or {}
            if (d.get("autopilot") or {}).get("enabled"):
                continue
            zones = max(1, len(d.get("locations") or [1]))
            groups, autoscale = [], False
            for pool in d.get("nodePools") or []:
                asc = pool.get("autoscaling") or {}
                autoscale |= bool(asc.get("enabled"))
                count = int(pool.get("initialNodeCount") or 0) * zones
                groups.append({"role": f"nodepool:{pool.get('name')}", "machine_type": (pool.get("config") or {}).get("machineType"),
                               "count": count, "preemptible": bool((pool.get("config") or {}).get("preemptible")
                                                                   or (pool.get("config") or {}).get("spot")),
                               "disk_gb": (pool.get("config") or {}).get("diskSizeGb"),
                               "disk_type": (pool.get("config") or {}).get("diskType") or "pd-standard",
                               "scalable": True, "min": asc.get("minNodeCount"), "max": asc.get("maxNodeCount")})
            clusters.append({"resource_id": f"cluster:gke:{d.get('name')}", "kind": "gke", "name": d.get("name"),
                             "location": d.get("location"), "state": d.get("status"), "node_groups": groups,
                             "autoscaling": autoscale, "labels": d.get("resourceLabels") or {},
                             "environment": (d.get("resourceLabels") or {}).get("env")})
        for a in self._assets("sqladmin.googleapis.com/Instance"):
            d = a.get("data") or {}
            st = d.get("settings") or {}
            labels = st.get("userLabels") or {}
            clusters.append({"resource_id": f"cluster:cloudsql:{d.get('name')}", "kind": "cloudsql", "name": d.get("name"),
                             "location": d.get("region"), "state": d.get("state"), "tier": st.get("tier"),
                             "engine": d.get("databaseVersion"), "high_availability": st.get("availabilityType") == "REGIONAL",
                             "disk_gb": float(st.get("dataDiskSizeGb") or 0), "disk_type": st.get("dataDiskType"),
                             "project_id": self.project_id, "labels": labels,
                             "environment": labels.get("env") or labels.get("environment"),
                             "database_id": f"{self.project_id}:{d.get('name')}"})
        self._attach_utilization(clusters)
        for c in clusters:
            c.setdefault("project_id", self.project_id)
        return clusters

    def _attach_utilization(self, clusters: list[dict[str, Any]]) -> None:
        kinds = {c["kind"] for c in clusters}
        m = self.metrics
        cpu: dict[str, dict[str, list[float]]] = {}
        mem: dict[str, dict[str, list[float]]] = {}
        try:
            if "dataproc" in kinds:
                cpu["dataproc"] = m.hourly("compute.googleapis.com/instance/cpu/utilization",
                                           'metadata.user_labels."goog-dataproc-cluster-name"',
                                           extra_filter='metadata.user_labels."goog-dataproc-cluster-name" != ""')
                mem["dataproc"] = m.hourly("dataproc.googleapis.com/cluster/yarn/allocated_memory_percentage",
                                           "resource.label.cluster_name")
            if "gke" in kinds:
                cpu["gke"] = m.hourly("kubernetes.io/node/cpu/allocatable_utilization", "resource.label.cluster_name")
                mem["gke"] = m.hourly("kubernetes.io/node/memory/allocatable_utilization", "resource.label.cluster_name")
            if "cloudsql" in kinds:
                cpu["cloudsql"] = m.hourly("cloudsql.googleapis.com/database/cpu/utilization", "resource.label.database_id")
                mem["cloudsql"] = m.hourly("cloudsql.googleapis.com/database/memory/utilization", "resource.label.database_id")
        except Exception as exc:  # noqa: BLE001
            self.errors.append(f"AVISO en 'monitoring': no se pudo leer el uso de clusters ({str(exc)[:200]}). "
                               "Rol necesario: roles/monitoring.viewer.")
        for c in clusters:
            key = c.get("database_id") if c["kind"] == "cloudsql" else c["name"]
            cs = util_stats((cpu.get(c["kind"]) or {}).get(key) or [])
            ms = util_stats((mem.get(c["kind"]) or {}).get(key) or [])
            if cs:
                c["utilization"] = {"cpu_p95": cs["p95"], "cpu_avg": cs["avg"], "active_hours_pct": cs["active_hours_pct"],
                                    "mem_p95": ms["p95"] if ms else None, "mem_avg": ms["avg"] if ms else None, "days": 14}

    # ---------- Microservicios (tráfico y costo)
    def services(self) -> list[dict[str, Any]]:
        arch = self.arch.get("architecture") or {}
        edges = arch.get("edges") or []
        nodes = {n["id"]: n for n in arch.get("nodes") or []}
        reqs, billable, fn_exec = {}, {}, {}
        try:
            reqs = self.metrics.total("run.googleapis.com/request_count", "resource.label.service_name", 30)
            billable = self.metrics.total("run.googleapis.com/container/billable_instance_time",
                                          "resource.label.service_name", 30)
            fn_exec = self.metrics.total("cloudfunctions.googleapis.com/function/execution_count",
                                         "resource.label.function_name", 30)
        except Exception as exc:  # noqa: BLE001
            logger.info("Métricas de servicios: %s", exc)
        out = []
        for w in self.arch.get("workloads") or []:
            if w["service_type"] not in ("Cloud Run", "Cloud Functions", "Cloud Run Job"):
                continue
            wid = w["resource_id"]
            inputs, outputs = [], []
            for e in edges:
                if e["target"] == wid and e["kind"] in ("push", "triggers"):
                    src = e["source"]
                    if src.startswith("sub:"):
                        src = next((x["source"] for x in edges if x["target"] == src and x["kind"] == "subscribes"), src)
                    if src.startswith("topic:"):
                        inputs.append(f"projects/{self.project_id}/topics/{src[6:]}")
                if e["source"] == wid and e["kind"] in ("connects_sql", "references", "writes_to"):
                    tgt = e["target"]
                    n = nodes.get(tgt, {})
                    if tgt.startswith("bucket:"):
                        inputs.append(f"gs://{tgt[7:]}")
                    elif tgt.startswith("topic:"):
                        outputs.append(f"projects/{self.project_id}/topics/{tgt[6:]}")
                    elif tgt.startswith("dataset:"):
                        inputs.append({"id": f"data:bqds:{self.project_id}.{tgt[8:]}", "system": "bigquery",
                                       "name": f"{self.project_id}.{tgt[8:]} (dataset)"})
                    elif tgt.startswith("sql:"):
                        eng = "postgres" if "POSTGRES" in str(n.get("engine", "")).upper() else "mysql" if "MYSQL" in str(n.get("engine", "")).upper() else "cloudsql"
                        inputs.append({"id": f"data:{eng}:{tgt[4:]}/", "system": eng, "name": f"Cloud SQL {tgt[4:]}"})
            name = w["name"]
            is_fn = w["service_type"] == "Cloud Functions"
            req = fn_exec.get(name) if is_fn else reqs.get(name)
            secs = billable.get(name)
            cpu, mem = P.parse_cloud_run_resources(w.get("resource_limits"))
            if secs is not None:
                cost, basis = P.cloud_run_cost(secs, cpu, mem), "Cloud Run (tiempo facturable 30 días)"
            elif w.get("min_instances"):
                idle_s = int(w["min_instances"]) * 30 * 86400      # instancias mínimas ociosas: tarifa reducida
                cost, basis = idle_s * (cpu + mem) * 0.0000025, "Cloud Run (instancias mínimas ociosas, estimado)"
            else:
                cost, basis = None, None
            out.append({"name": name, "service_type": w["service_type"], "runtime_service_account": w.get("runtime_service_account"),
                        "inputs": inputs, "outputs": outputs, "request_count_30d": int(req) if req is not None else None,
                        "billable_seconds_30d": secs, "cost_30d_usd": round(cost, 2) if cost is not None else None,
                        "cost_basis": basis})
        return out

    # ---------- orquestación
    def collect(self, describe: Callable[[str, Exception], str]) -> tuple[dict[str, Any], list[str], list[str]]:
        raw: dict[str, Any] = {"project_id": self.project_id, "self_email": self.self_email}
        meta = self._section("bigquery_metadata", self.bigquery_metadata, describe, {}) \
            if self._api_on("bigquery.googleapis.com") else {}
        raw.update(meta)
        locations = meta.get("bq_locations") or ["US"]
        if self._api_on("bigquery.googleapis.com"):
            raw.update(self._section("bigquery_jobs", lambda: self.bigquery_jobs(locations), describe, {}))
        if self._api_on("bigquerydatatransfer.googleapis.com"):
            raw["transfers"] = self._section("bigquery_transfers", lambda: self.transfers(locations), describe, [])
        if self._api_on("dataflow.googleapis.com"):
            raw["dataflow_jobs"] = self._section("dataflow", self.dataflow, describe, [])
        dproc = self._section("dataproc", self.dataproc, describe, {"clusters": [], "jobs": []}) \
            if self._api_on("dataproc.googleapis.com") else {"clusters": [], "jobs": []}
        raw["dataproc_jobs"] = dproc["jobs"]
        comp = self._section("composer", lambda: self.composer(describe), describe, {"environments": [], "dags": []}) \
            if self._assets("composer.googleapis.com/Environment") else {"environments": [], "dags": []}
        raw["composer_dags"] = comp["dags"]
        raw["datastream"] = self._section("datastream", self.datastream, describe, [])
        raw["lake_buckets"] = self._section("data_lake", self.lake, describe, []) if self.known_buckets else []
        raw["services"] = self._section("services_metrics", self.services, describe, [])
        raw["clusters"] = self._section("capacity", lambda: self.capacity(dproc["clusters"], comp["environments"]),
                                        describe, [])
        return raw, self.errors, self.ok
