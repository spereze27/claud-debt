"""Linaje de la POC de Terraform (terraform/poc_flujos_encadenados): lo que el agente recolectaría tras desplegarla.

Las formas de los datos imitan a GCP: TransferConfig.params (Struct → dict de strings), filas de
INFORMATION_SCHEMA.JOBS (labels como lista key/value, jobs hijos de scripts con parent_job_id) y
INFORMATION_SCHEMA.VIEWS. Se incluyen filas con campos nulos o vacíos que GCP devuelve a veces.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

from backend.core.data_platform import build_data_platform
from backend.core.excel_report import build_frames, clean_cell, write_excel
from backend.core.lineage import (NOT_DETECTED, assign_cascades, group_from_name, infer_layer_from, parse_label_string,
                                  query_label_from_sql, safe_labels)

P = "poc-proj"
BKT = f"{P}-landing-a1b2c3"
NOW = datetime.now(timezone.utc)
A = {"grupo_cascada": "ingesta-core", "owner": "finanzas", "poc": "flujos-encadenados"}
B = {"grupo_cascada": "reporting-aislado", "owner": "marketing"}

F2_SQL = f'''SET @@query_label = "grupo_cascada:ingesta-core,owner:finanzas,flujo:f2-bronze-a-silver,capa_entrada:bronze,capa_salida:silver";
MERGE `{P}.silver_finanzas.ventas` AS t USING (SELECT venta_id FROM `{P}.bronze_finanzas.ventas_raw` WHERE venta_id IS NOT NULL) AS s
ON t.venta_id = s.venta_id WHEN NOT MATCHED THEN INSERT (venta_id) VALUES (s.venta_id);'''
B1_SQL = f'''SET @@query_label = "grupo_cascada:reporting-aislado,owner:marketing,flujo:b1-kpi-marketing";
CREATE OR REPLACE TABLE `{P}.silver_finanzas.kpi_marketing` AS
SELECT corpus, SUM(word_count) AS palabras FROM `bigquery-public-data.samples.shakespeare` GROUP BY corpus;'''


def t(ds, name, labels, type_="BASE TABLE"):
    return {"project": P, "dataset": ds, "table": name, "type": type_, "columns": [{"name": "x", "type": "STRING"}],
            "labels": labels, "logical_bytes": 1024, "active_logical": 1024}


def job(jid, days, dest, refs, stmt, labels, job_type="QUERY", parent=None, user=None):
    start = NOW - timedelta(days=days)
    return {"job_id": jid, "parent_job_id": parent, "creation_time": start.isoformat(),
            "end_time": (start + timedelta(minutes=2)).isoformat(), "user_email": user or f"poc-flujos-dts@{P}.iam.gserviceaccount.com",
            "job_type": job_type, "statement_type": stmt, "state": "DONE", "error_reason": None, "destination": dest,
            "referenced": refs, "bytes_billed": 10 * 1024 ** 2, "slot_ms": 500, "labels": labels, "query": None}


def poc_raw():
    lab_a = [{"key": k, "value": v} for k, v in {**A, "flujo": "f2-bronze-a-silver"}.items()]
    lab_b = [{"key": k, "value": v} for k, v in {**B, "flujo": "b1-kpi-marketing"}.items()]
    jobs = []
    for d in range(3):
        jobs.append(job(f"bqts_f1_{d}", d, f"{P}.bronze_finanzas.ventas_raw", None, None,
                        [{"key": "data_source_id", "value": "google_cloud_storage"}], job_type="LOAD"))
        jobs.append(job(f"scheduled_query_f2_{d}", d, None, None, "SCRIPT", None))          # padre del script
        jobs.append(job(f"script_job_f2_{d}_1", d, f"{P}.silver_finanzas.ventas",
                        [f"{P}.bronze_finanzas.ventas_raw", f"{P}.silver_finanzas.ventas"], "MERGE", lab_a,
                        parent=f"scheduled_query_f2_{d}"))
    jobs.append(job("script_job_b1_1", 1, f"{P}.silver_finanzas.kpi_marketing", ["bigquery-public-data.samples.shakespeare"],
                    "CREATE_TABLE_AS_SELECT", lab_b, parent="scheduled_query_b1"))
    # Filas degeneradas que GCP puede devolver: todo nulo, destino malformado, labels vacías, referenced con None
    jobs += [{"job_id": None, "destination": None, "referenced": None, "labels": None, "state": None},
             job("weird_1", 1, "sin_puntos", [None, ""], None, [], user="analista@empresa.com"),
             job("adhoc_1", 1, f"{P}._script123.anon", [f"{P}.silver_finanzas.ventas"], "SELECT", None, user="bi@empresa.com")]
    return {
        "project_id": P, "self_email": f"auditor@{P}.iam.gserviceaccount.com",
        "bq_datasets": [{"project": P, "dataset": "bronze_finanzas", "labels": {**A, "capa": "bronze"}},
                        {"project": P, "dataset": "silver_finanzas", "labels": {"capa": "silver", "owner": "finanzas"}}],
        "bq_tables": [t("bronze_finanzas", "ventas_raw", {**A, "capa": "bronze"}),
                      t("silver_finanzas", "ventas", {**A, "capa": "silver"}),
                      t("silver_finanzas", "v_resumen_ventas_region", {**A, "capa": "silver", "flujo": "f3-vista-resumen"}, "VIEW"),
                      t("silver_finanzas", "kpi_marketing", {**B, "capa": "silver"})],
        "bq_views": [{"project": P, "dataset": "silver_finanzas", "view": "v_resumen_ventas_region",
                      "definition": f"SELECT region, fecha, COUNT(*) FROM `{P}.silver_finanzas.ventas` GROUP BY region, fecha"},
                     {"project": P, "dataset": None, "view": None, "definition": None}],
        "transfers": [
            {"name": f"projects/123/locations/us/transferConfigs/f1", "display_name": "[ingesta-core] f1_landing_a_bronze_ventas",
             "data_source_id": "google_cloud_storage", "destination_dataset": "bronze_finanzas", "schedule": "every day 02:00",
             "disabled": False, "state": "SUCCEEDED",
             "params": {"data_path_template": f"gs://{BKT}/ventas/*.csv", "destination_table_name_template": "ventas_raw",
                        "file_format": "CSV", "write_disposition": "APPEND", "skip_leading_rows": "1"}, "runs": []},
            {"name": f"projects/123/locations/us/transferConfigs/f2", "display_name": "[ingesta-core] f2_bronze_a_silver_ventas",
             "data_source_id": "scheduled_query", "destination_dataset": "", "schedule": "every day 03:00",
             "disabled": False, "state": "SUCCEEDED", "params": {"query": F2_SQL}, "runs": []},
            {"name": f"projects/123/locations/us/transferConfigs/b1", "display_name": "[reporting-aislado] b1_kpi_marketing_independiente",
             "data_source_id": "scheduled_query", "destination_dataset": None, "schedule": "every monday 08:00",
             "disabled": False, "state": "SUCCEEDED", "params": {"query": B1_SQL}, "runs": None},
            {"name": None, "display_name": None, "params": None},                     # config corrupta: se ignora
        ],
        "bq_jobs": jobs,
        "lake_buckets": [], "bucket_labels": {BKT: {**A, "capa": "landing"}},
    }


@pytest.fixture(scope="module")
def dp():
    return build_data_platform(poc_raw(), history_days=30)


def _by_name(dp):
    return {f["name"]: f for f in dp["flows"]}


def test_helpers_handle_nulls():
    assert safe_labels(None) == {} and safe_labels([{"key": None, "value": "x"}, {"key": "A", "value": None}]) == {"a": ""}
    assert safe_labels([("grupo", "X")]) == {"grupo": "x"}
    assert parse_label_string("grupo_cascada:ingesta-core, owner:finanzas,roto") == {"grupo_cascada": "ingesta-core",
                                                                                     "owner": "finanzas"}
    assert query_label_from_sql(F2_SQL)["capa_salida"] == "silver" and query_label_from_sql(None) == {}
    assert group_from_name("[ingesta-core] f1") == "ingesta-core" and group_from_name("ventas__x") == "ventas"
    assert group_from_name(None) is None and group_from_name("sin grupo") is None
    assert infer_layer_from(None, None, None) == (NOT_DETECTED, "sin evidencia en etiquetas ni nombres")
    assert infer_layer_from("x", None, {"capa": "trusted"})[0] == "silver"
    assert infer_layer_from(f"{BKT}")[0] == "landing"
    deps, groups = assign_cascades([], {})
    assert deps == [] and groups == []


def test_flows_detected_and_merged(dp):
    flows = _by_name(dp)
    assert set(flows) == {"f1_landing_a_bronze_ventas", "f2_bronze_a_silver_ventas", "v_resumen_ventas_region",
                          "b1_kpi_marketing_independiente"}, sorted(flows)
    assert flows["f1_landing_a_bronze_ventas"]["runs_in_window"] == 3        # jobs LOAD unidos a su transferencia
    assert flows["f2_bronze_a_silver_ventas"]["runs_in_window"] == 3         # jobs hijos del script unidos por parent_job_id


def test_layers_and_raw_io(dp):
    f = _by_name(dp)
    f1, f2, f3, b1 = (f["f1_landing_a_bronze_ventas"], f["f2_bronze_a_silver_ventas"], f["v_resumen_ventas_region"],
                      f["b1_kpi_marketing_independiente"])
    assert (f1["capa_entrada_inferida"], f1["capa_salida_inferida"]) == ("landing", "bronze")
    assert f1["fuente_raw"] == f"gs://{BKT}/ventas/*.csv" and f1["outputs"][0]["write_mode"] == "append"
    assert (f2["capa_entrada_inferida"], f2["capa_salida_inferida"]) == ("bronze", "silver")
    assert "etiqueta del flujo" in f2["origen_capa_salida"] and f2["outputs"][0]["write_mode"] == "merge"
    assert (f3["capa_entrada_inferida"], f3["capa_salida_inferida"]) == ("silver", "silver")
    assert b1["capa_entrada_inferida"] == NOT_DETECTED and b1["fuente_raw"] == "bigquery-public-data.samples.shakespeare"
    assert b1["outputs"][0]["write_mode"] == "overwrite"


def test_cascades_and_dependencies(dp):
    f = _by_name(dp)
    for name, order in (("f1_landing_a_bronze_ventas", 1), ("f2_bronze_a_silver_ventas", 2), ("v_resumen_ventas_region", 3)):
        assert f[name]["grupo_cascada_id"] == "ingesta-core" and f[name]["orden_en_cascada"] == order, name
        assert f[name]["owner"] == "finanzas"
    b1 = f["b1_kpi_marketing_independiente"]
    assert b1["grupo_cascada_id"] == "reporting-aislado" and b1["orden_en_cascada"] == 1 and b1["owner"] == "marketing"
    assert f["f1_landing_a_bronze_ventas"]["origen_grupo"] == "convención de nombre [grupo]"     # DTS no admite labels
    assert f["f2_bronze_a_silver_ventas"]["origen_grupo"] == "etiqueta del flujo/job"           # @@query_label
    pairs = {(d["origen_nombre"], d["destino_nombre"]) for d in dp["dependencies"]}
    assert pairs == {("f1_landing_a_bronze_ventas", "f2_bronze_a_silver_ventas"),
                     ("f2_bronze_a_silver_ventas", "v_resumen_ventas_region")}
    assert all(d["mismo_grupo"] for d in dp["dependencies"])
    g = {x["grupo_cascada_id"]: x for x in dp["cascades"]}
    assert g["ingesta-core"]["es_cascada"] and g["ingesta-core"]["profundidad"] == 3
    assert g["ingesta-core"]["capas_recorridas"] == "landing → bronze → silver"
    assert g["reporting-aislado"]["num_flujos"] == 1 and not g["reporting-aislado"]["es_cascada"]


def test_unlabeled_flows_get_auto_group():
    raw = poc_raw()
    raw["transfers"][1]["display_name"] = "f2_sin_grupo"
    raw["transfers"][1]["params"]["query"] = F2_SQL.split(";", 1)[1]                # sin @@query_label
    for j in raw["bq_jobs"]:
        if j.get("job_id", "") and str(j["job_id"]).startswith("script_job_f2"):
            j["labels"] = None
    dp = build_data_platform(raw, 30)
    f2 = _by_name(dp)["f2_sin_grupo"]
    # La tabla que escribe está etiquetada → hereda el grupo de los recursos
    assert f2["grupo_cascada_id"] == "ingesta-core" and f2["origen_grupo"] == "etiquetas de los recursos de salida"
    raw2 = poc_raw()
    for tb in raw2["bq_tables"]:
        tb["labels"] = {}
    for d in raw2["bq_datasets"]:
        d["labels"] = {}
    raw2["bucket_labels"] = {}
    for tr in raw2["transfers"][:3]:
        tr["display_name"] = tr["display_name"].split("] ")[1]
        if (tr.get("params") or {}).get("query"):
            tr["params"]["query"] = tr["params"]["query"].split(";", 1)[1]
    for j in raw2["bq_jobs"]:
        if isinstance(j.get("labels"), list):
            j["labels"] = [x for x in j["labels"] if x["key"] == "data_source_id"]
    dp2 = build_data_platform(raw2, 30)
    groups = {f["name"]: f["grupo_cascada_id"] for f in dp2["flows"]}
    assert groups["f1_landing_a_bronze_ventas"] == groups["f2_bronze_a_silver_ventas"] == groups["v_resumen_ventas_region"]
    assert groups["f1_landing_a_bronze_ventas"].startswith("auto-")
    assert groups["b1_kpi_marketing_independiente"] != groups["f1_landing_a_bronze_ventas"]    # componente aislado
    assert any(i["code"] == "FLOW_UNGROUPED" for i in dp2["issues"])


def test_excel_export(dp, tmp_path):
    state = {"provider": "GCP", "scope": "project", "target_id": P, "discovery_status": "complete",
             "raw_infra_data": {"data_platform": dp},
             "findings": [{"severity": "HIGH", "category": "pipelines", "rule": "FLOW_CONFLICT", "resource_id": "x",
                           "title": "=HYPERLINK(\"http://evil\")", "description": "a\x00b", "recommendation": None,
                           "estimated_monthly_waste_usd": 0}], "kpis": {}, "errors": []}
    frames = build_frames(state)
    path = tmp_path / "out.xlsx"
    write_excel(frames, str(path))
    xl = pd.read_excel(path, sheet_name=None)
    assert list(xl) == ["00_Resumen", "01_Inventario_Flujos", "02_Dependencias_Flujos", "03_Lineaje",
                        "04_Grupos_Cascada", "05_Hallazgos"]
    inv = xl["01_Inventario_Flujos"]
    for col in ("ID", "Nombre", "Capa (entrada → salida)", "Fuente (raw)", "Destino (raw)", "Grupo cascada"):
        assert col in inv.columns
    assert len(inv) == 4 and inv["Nombre"].tolist()[:3] == ["f1_landing_a_bronze_ventas", "f2_bronze_a_silver_ventas",
                                                             "v_resumen_ventas_region"]   # ordenado por grupo y orden
    dep = xl["02_Dependencias_Flujos"]
    assert {"origen_id", "destino_id"} <= set(dep.columns) and len(dep) == 2
    assert xl["05_Hallazgos"]["titulo"][0].startswith("'=")                     # fórmula neutralizada
    assert xl["05_Hallazgos"]["descripcion"][0] == "ab"                          # carácter de control eliminado


def test_excel_with_empty_state(tmp_path):
    frames = build_frames({})
    write_excel(frames, str(tmp_path / "vacio.xlsx"))
    xl = pd.read_excel(tmp_path / "vacio.xlsx", sheet_name=None)
    assert len(xl["01_Inventario_Flujos"]) == 0 and "Grupo cascada" in xl["01_Inventario_Flujos"].columns
    assert clean_cell(None) == "" and clean_cell(["a", None, "b"]) == "a; b" and clean_cell(3) == 3


def test_excel_node_and_api(dp, monkeypatch, tmp_path):
    from fastapi.testclient import TestClient

    from backend.agent_graph.nodes import excel_export
    from backend.main import app

    monkeypatch.setattr(excel_export, "REPORTS_DIR", str(tmp_path))
    out = excel_export.excel_export_node({"target_id": P, "raw_infra_data": {"data_platform": dp}, "findings": []})
    rep = out["excel_report"]
    assert rep["sheets"]["01_Inventario_Flujos"] == 4 and os.path.isfile(rep["path"])
    client = TestClient(app)
    r = client.get(f"/api/v1/reports/{rep['report_id']}.xlsx")
    assert r.status_code == 200 and r.content[:2] == b"PK"
    assert client.get("/api/v1/reports/..%2F..%2Fetc%2Fpasswd.xlsx").status_code == 404
    assert client.get("/api/v1/reports/" + "0" * 32 + ".xlsx").status_code == 404
