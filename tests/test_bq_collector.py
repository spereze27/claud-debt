"""Colector de BigQuery con un cliente simulado que devuelve filas con la forma de INFORMATION_SCHEMA."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace as NS

from backend.agent_graph.nodes.gcp_data import GCPDataCollector
from backend.core.data_platform import build_data_platform

NOW = datetime.now(timezone.utc)


class FakeBQ:
    def list_datasets(self):
        return [NS(reference="raw_ventas"), NS(reference="gold")]

    def get_dataset(self, ref, timeout=None):
        return NS(dataset_id=ref, location="US", labels={"layer": "bronze"} if ref == "raw_ventas" else {},
                  description=None, storage_billing_model="LOGICAL")

    def query(self, sql, job_config=None, location=None):
        s = " ".join(sql.split())
        if "INFORMATION_SCHEMA.TABLES" in s:
            rows = [{"table_schema": "raw_ventas", "table_name": "orders", "table_type": "BASE TABLE", "creation_time": NOW},
                    {"table_schema": "gold", "table_name": "daily", "table_type": "BASE TABLE", "creation_time": NOW}]
        elif "TABLE_OPTIONS" in s:
            rows = [{"table_schema": "gold", "table_name": "daily", "option_name": "labels",
                     "option_value": '[STRUCT("layer", "gold")]'},
                    {"table_schema": "gold", "table_name": "daily", "option_name": "description", "option_value": '"Ventas por día"'}]
        elif "TABLE_STORAGE" in s:
            rows = [{"table_schema": "raw_ventas", "table_name": "orders", "total_rows": 10, "total_logical_bytes": 5 * 1024 ** 3,
                     "active_logical_bytes": 5 * 1024 ** 3, "long_term_logical_bytes": 0, "active_physical_bytes": 1024 ** 3,
                     "long_term_physical_bytes": 0, "storage_last_modified_time": NOW}]
        elif "INFORMATION_SCHEMA.COLUMNS" in s:
            rows = [{"table_schema": "raw_ventas", "table_name": "orders", "column_name": "fecha", "data_type": "STRING",
                     "is_partitioning_column": "NO", "clustering_ordinal_position": None},
                    {"table_schema": "gold", "table_name": "daily", "column_name": "dia", "data_type": "DATE",
                     "is_partitioning_column": "YES", "clustering_ordinal_position": None},
                    {"table_schema": "gold", "table_name": "daily", "column_name": "tags", "data_type": "ARRAY<STRING>",
                     "is_partitioning_column": "NO", "clustering_ordinal_position": 1}]
        elif "JOBS_BY_PROJECT" in s:
            assert "@days" in s and job_config.query_parameters[0].value == 30
            rows = [{"job_id": f"j{i}", "creation_time": NOW - timedelta(days=i), "end_time": NOW - timedelta(days=i) + timedelta(minutes=3),
                     "user_email": "etl@p.iam.gserviceaccount.com", "job_type": "QUERY", "statement_type": "SELECT", "state": "DONE",
                     "error_reason": None, "destination": "proj-1.gold.daily", "referenced": ["proj-1.raw_ventas.orders"],
                     "bytes_billed": 1024 ** 3, "slot_ms": 100, "cache_hit": False,
                     "labels": [{"key": "airflow-dag", "value": "daily_dag"}], "query": "SELECT ..."} for i in range(5)]
        else:
            raise AssertionError(s)
        return NS(result=lambda timeout=None: rows)

    def get_job(self, job_id, location=None, timeout=None):
        return NS(write_disposition="WRITE_TRUNCATE", source_uris=None, source_format=None)


def test_bigquery_collector(monkeypatch):
    col = GCPDataCollector(NS(service_account_email="auditor@p.iam.gserviceaccount.com"), "proj-1", [], {})
    monkeypatch.setattr(col, "_bq", lambda: FakeBQ())
    meta = col.bigquery_metadata()
    t = {x["table"]: x for x in meta["bq_tables"]}
    assert t["daily"]["labels"] == {"layer": "gold"} and t["daily"]["description"] == "Ventas por día"
    assert t["daily"]["partition_column"] == "dia" and t["daily"]["clustering"] == ["tags"]
    assert t["daily"]["columns"][1]["mode"] == "REPEATED" and t["orders"]["logical_bytes"] == 5 * 1024 ** 3
    jobs = col.bigquery_jobs(meta["bq_locations"])
    assert len(jobs["bq_jobs"]) == 5 and jobs["bq_jobs"][0]["labels"] == {"airflow-dag": "daily_dag"}
    assert jobs["bq_job_details"]["j0"]["write_disposition"] == "WRITE_TRUNCATE"   # 1 muestra por (identidad, destino)
    dp = build_data_platform({"project_id": "proj-1", **meta, **jobs}, 30)
    f = dp["flows"][0]
    assert f["name"] == "daily_dag" and f["outputs"][0]["write_mode"] == "overwrite"
    assert f["inputs"][0]["layer"] == "bronze" and f["outputs"][0]["layer"] == "gold" and f["runs_in_window"] == 5
    assert any(i["code"] == "LAYER_SKIP" for i in dp["issues"])            # gold directo desde bronze
