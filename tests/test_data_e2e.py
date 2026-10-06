"""Grafo completo con el escenario de datos + parsers de los colectores (sin red, LLM falso)."""
from __future__ import annotations

from datetime import datetime, timezone

import pytest
from langchain_core.messages import AIMessage

from backend.agent_graph import graph as graph_mod
from backend.agent_graph.nodes import base, data_report, report
from backend.agent_graph.nodes.gcp_data import dataflow_io, io_from_args, parse_dag_source, parse_labels_option, summarize_objects
from backend.core.data_platform import build_data_platform
from backend.core.rightsizing import size_all
from tests.test_data_platform import CLUSTERS, RAW


class QuietLLM:
    """Los agentes no reportan nada (prueba que los hallazgos deterministas cubren); los informes devuelven texto."""

    def __init__(self):
        self.schema = None

    def with_structured_output(self, schema):
        self.schema = schema
        return self

    def invoke(self, messages):
        return self.schema(findings=[]) if self.schema else AIMessage(content="# Informe\nNarrativa del LLM.")


@pytest.fixture
def data_graph(monkeypatch):
    sized = size_all(CLUSTERS)
    dp = build_data_platform({**RAW, "clusters_sized": sized}, 10)
    dp["clusters"] = sized
    raw = {"metadata": {"collector": "gcp-sdk-v1"}, "data_platform": dp,
           "projects": [{"project_id": "proj-1", "folder_path": "org / ventas"}]}
    monkeypatch.setattr(graph_mod, "discovery_node", lambda state: {"raw_infra_data": raw, "discovery_status": "complete"})
    for mod in (base, report, data_report):
        monkeypatch.setattr(mod, "get_llm_from_config", lambda cfg: QuietLLM())
    graph_mod.get_compiled_graph.cache_clear()
    yield
    graph_mod.get_compiled_graph.cache_clear()


def test_graph_with_data_platform(data_graph):
    out = graph_mod.get_compiled_graph().invoke(
        {"provider": "GCP", "scope": "project", "target_id": "proj-1", "findings": [], "errors": []},
        {"configurable": {"llm_api_key": "x"}})
    cats = {f["category"] for f in out["findings"]}
    assert {"data_governance", "pipelines", "capacity"} <= cats
    rules = {f["rule"] for f in out["findings"]}
    assert {"FLOW_CONFLICT", "BRONZE_OVERWRITE", "CLUSTER_OVERSIZED", "DB_UNDERSIZED", "NORMALIZATION_ISSUE",
            "WRONG_FILE_FORMAT"} <= rules
    k = out["kpis"]
    assert k["capacity"]["oversized"] == 1 and k["capacity"]["potential_saving_usd"] > 0
    assert k["pipelines"]["conflicts"] >= 1 and k["debt_scores"]["pipelines"] > 0
    assert k["cost"]["monthly_waste_usd"] > 0
    rep = out["data_report"]
    assert rep.startswith("# Informe") and "Anexo técnico" in rep and "Proyecto `proj-1`" in rep
    assert "carpeta `org / ventas`" in rep and "Dimensionamiento de clusters" in rep
    assert not out["errors"]


def test_parse_dag_source():
    code = '''
from airflow import DAG
with DAG("ventas_diarias", schedule_interval="0 3 * * *") as dag:
    extract = PostgresToGCSOperator(task_id="e", postgres_conn_id="pg_ventas", sql="SELECT * FROM ventas",
                                    bucket="landing-raw", filename="ventas/{{ ds }}/data.json")
    load = GCSToBigQueryOperator(task_id="l", bucket="landing-raw", source_objects=["ventas/*.json"],
                                 destination_project_dataset_table="proj-1.raw_ventas.ventas",
                                 write_disposition="WRITE_TRUNCATE")
    gold = BigQueryInsertJobOperator(task_id="g", configuration={"query": {"query": """
        INSERT INTO `proj-1.gold.ventas_dia` SELECT fecha, SUM(total) FROM `proj-1.silver.ventas` GROUP BY 1""",
        "useLegacySql": False}})
'''
    d = parse_dag_source(code, "proj-1")[0]
    assert d["dag_id"] == "ventas_diarias" and d["schedule"] == "0 3 * * *"
    assert d["outputs"]["proj-1.raw_ventas.ventas"] == "overwrite"
    assert d["outputs"]["`proj-1.gold.ventas_dia`"] == "append"
    assert "postgresql://pg_ventas/" in d["inputs"] and "gs://landing-raw/ventas/*.json" in d["inputs"]
    assert "`proj-1.silver.ventas`" in d["inputs"]
    assert {"PostgresToGCSOperator", "GCSToBigQueryOperator", "BigQueryInsertJobOperator"} <= set(d["operators"])


def test_collector_parsers():
    ins, outs, unk, mode = io_from_args(["--input", "hdfs:///data/logs", "--output=gs://lake/silver/logs",
                                         "gs://other/x", "--mode", "overwrite"])
    assert ins == ["hdfs:///data/logs"] and outs == ["gs://lake/silver/logs"] and unk == ["gs://other/x"]
    assert mode == "overwrite"
    job = {"jobMetadata": {"pubsubDetails": [{"subscription": "projects/p/subscriptions/s1"}]},
           "pipelineDescription": {"originalPipelineTransform": [
               {"name": "WriteToBigQuery", "displayData": [{"strValue": "proj-1:silver.eventos"}]},
               {"name": "ParseJson", "displayData": [{"strValue": "gs://tmp/x"}]}]}}
    assert dataflow_io(job) == (["projects/p/subscriptions/s1"], ["proj-1.silver.eventos"])
    assert parse_labels_option('[STRUCT("layer", "gold"), STRUCT("owner", "ana")]') == {"layer": "gold", "owner": "ana"}
    now = datetime.now(timezone.utc)
    s = summarize_objects([("bronze/ventas/dt=2024-01-01/a.parquet", 1024, now),
                           ("bronze/ventas/dt=2024-01-02/b.parquet", 2048, now),
                           ("silver/x.csv", 10 * 1024 ** 3, now)], now)
    assert s["partition_style"] and set(s["top_prefixes"]) == {"bronze", "silver"}
    assert s["ext_bytes"]["csv"] == 10 * 1024 ** 3 and s["small_files_pct"] == 66.7
