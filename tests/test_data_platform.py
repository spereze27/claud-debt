"""Tests del motor determinista de datos: capas, normalización, linaje, conflictos, costos y dimensionamiento."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from backend.core import pricing as P
from backend.core.data_platform import build_data_platform, classify_layer, normalize_ref, sql_io
from backend.core.rightsizing import size_all

NOW = datetime.now(timezone.utc)
PID = "proj-1"


def ts(days_ago: float, minutes: float = 0) -> str:
    return (NOW - timedelta(days=days_ago) + timedelta(minutes=minutes)).isoformat()


def col(name, type_="STRING", mode="NULLABLE"):
    return {"name": name, "type": type_, "mode": mode}


def table(ds, t, cols, gb=0.1, part=None, labels=None):
    b = gb * P.GIB
    return {"project": PID, "dataset": ds, "table": t, "type": "BASE TABLE", "columns": cols, "logical_bytes": b,
            "active_logical": b, "lt_logical": 0, "active_physical": b / 4, "lt_physical": 0, "partition_column": part,
            "clustering": [], "labels": labels or {}, "description": None, "last_modified": ts(1)}


TABLES = [
    table("raw_ventas", "orders_cdc", [col("order_id"), col("payload"), col("fecha_carga")], gb=200, part="_PARTITIONTIME"),
    table("raw_ventas", "customers", [col("customer_id"), col("email"), col("telefono1"), col("telefono2")]),
    table("silver", "orders", [col("order_id"), col("customer_id"), col("customer_name"), col("product_id"),
                               col("product_name"), col("amount"), col("order_date")], gb=5),
    table("gold", "sales_daily", [col("dia", "DATE"), col("customer_id"), col("store_id"), col("total", "NUMERIC")], gb=20),
    table("gold", "kpi", [col("k"), col("v", "FLOAT64")]),
    table("gold", "a", [col("x")]),
    table("silver", "b", [col("x")]),
]
DATASETS = [{"project": PID, "dataset": d, "location": "US", "labels": {}, "storage_billing_model": "LOGICAL"}
            for d in ("raw_ventas", "silver", "gold")]


def job(jid, days_ago, dest, refs, stmt, query="", user="etl@proj-1.iam.gserviceaccount.com", gb=1.0, labels=None,
        job_type="QUERY", minutes=5, state="DONE", err=None):
    return {"job_id": jid, "creation_time": ts(days_ago), "end_time": ts(days_ago, minutes), "user_email": user,
            "job_type": job_type, "statement_type": stmt, "state": state, "error_reason": err, "destination": dest,
            "referenced": refs, "bytes_billed": int(gb * P.GIB), "slot_ms": 1000, "labels": labels or {}, "query": query}


JOBS = []
for d in range(10):  # scheduled query diaria bronze → silver (MERGE)
    JOBS.append(job(f"scheduled_query_{d}", d, f"{PID}.silver.orders", [f"{PID}.raw_ventas.orders_cdc", f"{PID}.silver.orders"],
                    "MERGE", "MERGE silver.orders USING raw_ventas.orders_cdc ...", gb=200,
                    labels={"data_source_id": "scheduled_query"}))
for d in range(10):  # DAG de Composer silver → gold (CREATE OR REPLACE)
    JOBS.append(job(f"airflow_{d}", d, f"{PID}.gold.sales_daily", [f"{PID}.silver.orders"], "CREATE_TABLE_AS_SELECT",
                    "CREATE OR REPLACE TABLE gold.sales_daily AS SELECT dia, SUM(amount) FROM silver.orders GROUP BY 1",
                    labels={"airflow-dag": "gold_daily"}, minutes=40 + d * 0))
JOBS.append(job("adhoc_1", 2, f"{PID}.gold.sales_daily", [f"{PID}.silver.orders"], "INSERT",
                "INSERT INTO gold.sales_daily SELECT ...", user="analyst@corp.com"))
JOBS.append(job("skip_1", 1, f"{PID}.gold.kpi", [f"{PID}.raw_ventas.orders_cdc"], "INSERT",
                "INSERT INTO gold.kpi SELECT COUNT(*) FROM raw_ventas.orders_cdc", user="bi@proj-1.iam.gserviceaccount.com"))
JOBS.append(job("load_1", 1, f"{PID}.raw_ventas.customers", [], None, job_type="LOAD", user="loader@proj-1.iam.gserviceaccount.com"))
JOBS.append(job("cyc_x", 1, f"{PID}.silver.b", [f"{PID}.gold.a"], "INSERT", "INSERT INTO silver.b SELECT * FROM gold.a", user="x@p.com"))
JOBS.append(job("cyc_y", 1, f"{PID}.gold.a", [f"{PID}.silver.b"], "INSERT", "INSERT INTO gold.a SELECT * FROM silver.b", user="y@p.com"))
JOBS.append(job("bi_1", 1, f"{PID}._anon.tmp", [f"{PID}.gold.sales_daily"], "SELECT", user="looker@p.com"))
JOBS.append(job("own", 1, f"{PID}.x.y", [], "SELECT", user="auditor@proj-1.iam.gserviceaccount.com"))
DETAILS = {"load_1": {"write_disposition": "WRITE_TRUNCATE", "source_uris": ["gs://landing-raw/customers/dt=2024-01-01/c.csv"],
                      "source_format": "CSV"}}

RAW = {
    "project_id": PID, "self_email": "auditor@proj-1.iam.gserviceaccount.com",
    "bq_tables": TABLES, "bq_datasets": DATASETS, "bq_jobs": JOBS, "bq_job_details": DETAILS,
    "transfers": [
        {"name": "projects/1/locations/us/transferConfigs/sq1", "display_name": "orders_to_silver",
         "data_source_id": "scheduled_query", "destination_dataset": "silver", "schedule": "every 24 hours",
         "disabled": False, "state": "SUCCEEDED",
         "params": {"query": "MERGE silver.orders USING raw_ventas.orders_cdc ...", "destination_table_name_template": "orders",
                    "write_disposition": "WRITE_APPEND"}, "runs": []},
        {"name": "projects/1/locations/us/transferConfigs/old", "display_name": "legacy_export",
         "data_source_id": "scheduled_query", "destination_dataset": "gold", "disabled": True, "params": {"query": "SELECT 1"}},
    ],
    "datastream": [{"name": "pg-cdc", "state": "RUNNING", "source": "postgresql://10.0.0.5/ventas",
                    "destination": f"{PID}.raw_ventas.orders_cdc"}],
    "dataproc_jobs": [{"id": "j1", "cluster": "etl-cluster", "type": "pyspark", "main": "gs://code/logs_job.py",
                       "state": "DONE", "start": ts(1), "end": ts(1, 120), "inputs": ["hdfs:///data/logs/2024/01"],
                       "outputs": ["gs://lake-silver/logs/dt=2024-01-01/"], "write_mode": "overwrite"}],
    "lake_buckets": [{"bucket": "landing-raw", "storage_class": "STANDARD", "lifecycle_rules": 0, "sampled_objects": 500,
                      "ext_bytes": {"csv": 50 * P.GIB}, "small_files_pct": 10, "avg_object_mb": 100,
                      "partition_style": True, "median_age_days": 200, "requests_30d": 10, "size_bytes": 500 * P.GIB,
                      "top_prefixes": ["customers", "orders"]},
                     {"bucket": "lake-silver", "storage_class": "STANDARD", "sampled_objects": 1000,
                      "ext_bytes": {"parquet": 2 * P.GIB, "json": 8 * P.GIB}, "small_files_pct": 90, "avg_object_mb": 2,
                      "partition_style": True, "median_age_days": 5, "size_bytes": 10 * P.GIB, "top_prefixes": ["logs"]}],
    "catalog": [{"name": "sftp_proveedores", "engine": "script on-prem", "inputs": [{"name": "sftp://prov/in", "system": "sftp"}],
                 "outputs": [{"name": "gs://landing-raw/proveedores/", "write_mode": "append", "format": "csv"}]}],
}
CLUSTERS = [{"resource_id": "cluster:dataproc:etl-cluster", "kind": "dataproc", "name": "etl-cluster", "state": "RUNNING",
             "node_groups": [{"role": "master", "machine_type": "n2-standard-8", "count": 1, "scalable": False},
                             {"role": "worker", "machine_type": "n2-standard-16", "count": 10, "scalable": True}],
             "autoscaling": False, "idle_delete_ttl_s": None, "hdfs_heavy": True,
             "utilization": {"cpu_p95": 18.0, "cpu_avg": 6.0, "mem_p95": 25.0, "mem_avg": 12.0, "active_hours_pct": 20, "days": 14}},
            {"resource_id": "cluster:cloudsql:pg", "kind": "cloudsql", "name": "pg", "tier": "db-custom-16-65536",
             "high_availability": True, "disk_gb": 100, "utilization": {"cpu_p95": 92.0, "cpu_avg": 70.0, "mem_p95": 80.0,
                                                                       "mem_avg": 60.0, "days": 14}}]


def _dp():
    return build_data_platform({**RAW, "clusters_sized": size_all(CLUSTERS)}, history_days=10)


def _flow(dp, name):
    return next(f for f in dp["flows"] if f["name"] == name)


def _codes(dp, fid=None):
    return {i["code"] for i in dp["issues"] if fid is None or fid in i["flows"] or i["resource_id"] == fid}


def test_helpers():
    assert classify_layer("orders_cdc", "raw_ventas")[0] == "bronze"
    assert classify_layer("x", None, {"layer": "gold"})[0] == "gold"
    assert classify_layer("stg_ventas", "mart")[0] == "silver"          # el nombre gana al contenedor
    assert normalize_ref("gs://b/raw/ventas/dt=2024-01-01/p.parquet")["id"] == "data:gcs:b/raw/ventas"
    assert normalize_ref("jdbc:postgresql://db.host:5432/ventas")["id"] == "data:postgres:db.host/ventas"
    assert normalize_ref("silver.orders", "p-12345")["id"] == "data:bq:p-12345.silver.orders"
    r, w = sql_io("CREATE OR REPLACE TABLE gold.x AS SELECT * FROM silver.a JOIN silver.b USING(id)", "p-12345")
    assert w == [("gold.x", "overwrite")] and set(r) == {"silver.a", "silver.b"}


def test_flows_and_io():
    dp = _dp()
    sq = _flow(dp, "orders_to_silver")
    assert sq["kind"] == "bq_scheduled_query" and sq["runs_in_window"] == 10          # jobs unidos a la config DTS
    assert {o["write_mode"] for o in sq["outputs"]} >= {"merge"}
    dag = _flow(dp, "gold_daily")
    assert dag["outputs"][0]["write_mode"] == "overwrite" and dag["layers_in"] == ["silver"]
    assert dag["avg_duration_s"] == 2400
    ds = _flow(dp, "pg-cdc")
    assert ds["inputs"][0]["system"] == "postgres" and ds["outputs"][0]["layer"] == "bronze"
    spark = _flow(dp, "logs_job.py")
    assert spark["inputs"][0]["system"] == "hdfs" and spark["cost_30d_usd"] > 0     # prorrateo del cluster
    assert any(f["kind"] == "catalog" for f in dp["flows"])
    assert not any("auditor" in (f.get("identity") or "") for f in dp["flows"])  # jobs propios excluidos
    # costo BigQuery: 200 GB × 10 ejecuciones en 10 días → proyectado a 30 días
    assert abs(sq["cost_30d_usd"] - 200 * 10 / 1024 * 6.25 * 3) < 0.5


def test_issues():
    dp = _dp()
    codes = _codes(dp)
    assert {"FLOW_CONFLICT", "FLOW_CYCLE", "LAYER_INVERSION", "LAYER_SKIP", "BRONZE_OVERWRITE", "FLOW_INACTIVE",
            "FULL_SCAN", "ORPHAN_OUTPUT"} <= codes, codes
    conflict = next(i for i in dp["issues"] if i["code"] == "FLOW_CONFLICT")
    assert "SOBRESCRIBE" in conflict["text"] and conflict["severity"] == "HIGH"
    assert _flow(dp, "legacy_export")["state"] == "inactive"


def test_tables_lake_and_costs():
    dp = _dp()
    t = {x["table"]: x for x in dp["tables"]}
    silver = t["orders"]
    assert silver["layer"] == "silver" and silver["normalization"] == "desnormalizada"
    assert any(s["code"] == "NORMALIZATION_ISSUE" for s in silver["signals"])
    assert any(s["code"] == "NO_PARTITIONING" for s in t["sales_daily"]["signals"])
    cust = t["customers"]
    assert cust["pii_columns"] and cust["repeating_groups"] == ["telefono"]
    assert "sales_daily" in [x.split(".")[-1] for x in t["orders"]["readers"]] or t["orders"]["readers"]
    lake = {b["bucket"]: b for b in dp["lake"]}
    assert {s["code"] for s in lake["landing-raw"]["signals"]} >= {"WRONG_STORAGE_CLASS", "NO_LIFECYCLE"}
    assert {s["code"] for s in lake["lake-silver"]["signals"]} >= {"WRONG_FILE_FORMAT", "SMALL_FILES"}
    cb = dp["cost_breakdown"]
    assert cb["total_usd"] == round(cb["flows_total_usd"] + cb["shared_total_usd"], 2)
    assert any("ociosa" in s["resource"] for s in cb["shared"])


def test_rightsizing():
    sized = {c["name"]: c["sizing"] for c in size_all(CLUSTERS)}
    dp = sized["etl-cluster"]
    assert dp["verdict"] == "oversized" and dp["monthly_delta_usd"] < 0
    assert "worker: 10 → 4 nodos" in dp["recommended_config"]
    assert any("efímero" in e["action"] for e in dp["additional_savings"])
    assert any("HDFS" in r for r in dp["reasons"])
    sql = sized["pg"]
    assert sql["verdict"] == "undersized" and sql["monthly_delta_usd"] > 0 and "db-custom-32" in sql["recommended_config"]
    assert P.parse_machine_type("n2-standard-8").memory_gb == 32 and P.parse_machine_type("e2-medium").vcpus == 1
