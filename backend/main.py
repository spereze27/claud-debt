"""API FastAPI que expone el grafo de auditoría.

Endpoints:
  GET  /health
  POST /api/v1/audit          → ejecución completa (JSON)
  POST /api/v1/audit/stream   → progreso nodo a nodo (NDJSON)

La API Key del LLM viaja en la cabecera `X-LLM-API-Key` (no en el body) y se
inyecta en `config["configurable"]`; nunca se guarda en el estado ni en logs.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
import tempfile
import time
import uuid
from contextlib import contextmanager
from typing import Any, AsyncIterator, Iterator, Literal

from fastapi import FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel, Field, field_validator

from backend.agent_graph.graph import NODE_SEQUENCE, get_compiled_graph
from backend.agent_graph.nodes.discovery import missing_gcp_modules
from backend.agent_graph.nodes.excel_export import report_path
from backend.core.llm_config import LLMConfigError, LLMFactory, LLMSettings

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("api")

app = FastAPI(title="Cloud Tech Debt Analyzer", version="1.4.0")
if _missing := missing_gcp_modules():
    logger.warning("Faltan librerías de GCP en %s: %s", sys.executable, _missing)

app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


class AuditRequest(BaseModel):
    provider: Literal["AWS", "GCP", "Azure"]
    scope: Literal["organization", "folder", "project"]
    target_id: str = Field(min_length=3, max_length=128)
    llm_provider: Literal["openai", "anthropic", "gemini"] = "openai"
    llm_model: str | None = None
    cloud_credentials: str | None = Field(default=None, description="JSON de la Service Account")
    billing_export_table: str | None = Field(default=None, max_length=300,
                                             description="Opcional: proyecto.dataset.tabla del export de facturación a BigQuery")
    history_days: int = Field(default=30, ge=1, le=180, description="Días de historial de jobs a analizar")
    sample_lake: bool = Field(default=True, description="Muestrear objetos de los buckets del data lake")
    read_dags: bool = Field(default=True, description="Leer el código de los DAGs de Composer")
    max_projects: int = Field(default=10, ge=1, le=50, description="Máximo de proyectos para carpeta/organización")
    flow_catalog: str | None = Field(default=None, max_length=500_000,
                                     description="YAML/JSON con flujos y fuentes externas (HDFS on-prem, Postgres…)")

    @field_validator("target_id")
    @classmethod
    def _strip(cls, v: str) -> str:
        v = v.strip()
        if not v.replace("-", "").replace("_", "").replace(".", "").isalnum():
            raise ValueError("target_id sólo admite letras, números, '-', '_' y '.'")
        return v


@contextmanager
def _credentials_file(raw: str | None) -> Iterator[str | None]:
    """Escribe el JSON de credenciales en un archivo temporal 0600 y lo borra siempre al salir."""
    if not raw:
        yield None
        return
    try:
        info = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=400, detail="Las credenciales cloud no son un JSON válido.") from exc
    if info.get("type") != "service_account":
        raise HTTPException(status_code=400, detail="El JSON debe ser una clave de Service Account (type=service_account).")
    fd, path = tempfile.mkstemp(prefix="sa_", suffix=".json")
    try:
        os.chmod(path, 0o600)
        with os.fdopen(fd, "w") as fh:
            json.dump(info, fh)
        yield path
    finally:
        try:
            os.remove(path)
        except OSError:
            logger.warning("No se pudo eliminar el archivo temporal de credenciales")


def parse_catalog(text: str | None) -> list[dict[str, Any]]:
    """Catálogo opcional de flujos. Acepta YAML o JSON: una lista o un objeto con clave `flows`."""
    if not text or not text.strip():
        return []
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        try:
            import yaml

            data = yaml.safe_load(text)
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=400, detail=f"El catálogo de flujos no es YAML/JSON válido: {exc}") from exc
    flows = data.get("flows") if isinstance(data, dict) else data
    if not isinstance(flows, list) or not all(isinstance(f, dict) for f in flows):
        raise HTTPException(status_code=400, detail="El catálogo debe ser una lista de flujos (o {flows: [...]}).")
    return flows[:500]


def _prepare(req: AuditRequest, api_key: str | None) -> tuple[dict[str, Any], dict[str, Any]]:
    if not api_key:
        raise HTTPException(status_code=401, detail="Falta la cabecera X-LLM-API-Key.")
    try:  # valida proveedor/key antes de lanzar el grafo
        LLMFactory.create(LLMSettings(provider=req.llm_provider, api_key=api_key, model=req.llm_model))
    except LLMConfigError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except ImportError as exc:
        raise HTTPException(status_code=500, detail=f"Dependencia no instalada: {exc.name}") from exc

    initial_state = {"provider": req.provider, "scope": req.scope, "target_id": req.target_id,
                     "has_credentials": bool(req.cloud_credentials), "findings": [], "errors": [],
                     "billing_export_table": (req.billing_export_table or "").strip() or None,
                     "data_options": {"history_days": req.history_days, "sample_lake": req.sample_lake,
                                      "read_dags": req.read_dags, "max_projects": req.max_projects,
                                      "catalog": parse_catalog(req.flow_catalog)}}
    config = {"configurable": {"llm_provider": req.llm_provider, "llm_api_key": api_key, "llm_model": req.llm_model},
              "run_name": f"audit-{req.provider}-{req.target_id}", "max_concurrency": 6}
    return initial_state, config


def _public_result(state: dict[str, Any]) -> dict[str, Any]:
    out = {k: state.get(k) for k in ("provider", "scope", "target_id", "raw_infra_data", "findings",
                                      "kpis", "final_report", "data_report", "errors", "discovery_status")}
    excel = state.get("excel_report")
    out["excel_report"] = {k: v for k, v in excel.items() if k != "path"} if excel else None  # nunca exponer rutas
    return out


@app.get("/health")
def health() -> dict[str, Any]:
    return {"status": "ok", "version": app.version, "nodes": NODE_SEQUENCE,
            "discovery_mode": os.getenv("DISCOVERY_MODE", "real").lower(),
            "missing_gcp_modules": missing_gcp_modules(), "python": sys.executable}


@app.post("/api/v1/audit")
async def run_audit(req: AuditRequest, x_llm_api_key: str | None = Header(default=None)) -> dict[str, Any]:
    state, config = _prepare(req, x_llm_api_key)
    try:
        with _credentials_file(req.cloud_credentials) as cred_path:
            state["credentials_path"] = cred_path
            result = await get_compiled_graph().ainvoke(state, config=config)
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        logger.exception("Auditoría falló")
        raise HTTPException(status_code=500, detail=f"Error ejecutando el grafo: {exc}") from exc
    return _public_result(result)


async def _graph_events(req: AuditRequest, state: dict[str, Any], config: dict[str, Any]) -> AsyncIterator[dict[str, Any]]:
    """Ejecuta el grafo y emite eventos {event, node, summary} y finalmente {event: completed, result}."""
    final: dict[str, Any] = dict(state)
    try:
        with _credentials_file(req.cloud_credentials) as cred_path:
            state["credentials_path"] = cred_path
            async for chunk in get_compiled_graph().astream(state, config=config, stream_mode="updates"):
                for node, update in chunk.items():
                    update = update or {}
                    for key, val in update.items():  # replica los reducers para el estado final
                        final[key] = final.get(key, []) + val if key in ("findings", "errors") else val
                    yield {"event": "node_completed", "node": node,
                           "summary": {"findings": len(update.get("findings", [])), "errors": update.get("errors", [])}}
        yield {"event": "completed", "result": _public_result(final)}
    except Exception as exc:  # noqa: BLE001
        logger.exception("Auditoría falló")
        yield {"event": "error", "detail": f"{type(exc).__name__}: {exc}"}


@app.post("/api/v1/audit/stream")
async def run_audit_stream(req: AuditRequest, x_llm_api_key: str | None = Header(default=None)) -> StreamingResponse:
    state, config = _prepare(req, x_llm_api_key)
    with _credentials_file(req.cloud_credentials):  # valida el JSON antes de abrir el stream (400 si es inválido)
        pass

    async def lines() -> AsyncIterator[str]:
        yield json.dumps({"event": "start", "nodes": NODE_SEQUENCE}) + "\n"
        async for ev in _graph_events(req, state, config):
            yield json.dumps(ev, ensure_ascii=False, default=str) + "\n"

    return StreamingResponse(lines(), media_type="application/x-ndjson")


# ------------------------------------------------------------------ jobs en segundo plano
# La UI lanza la auditoría y consulta su progreso con peticiones cortas: no depende de
# mantener una conexión abierta, así que la interfaz nunca queda bloqueada.
JOBS: dict[str, dict[str, Any]] = {}
JOB_TTL_SECONDS = 3600
_TASKS: set[asyncio.Task] = set()


def _purge_jobs() -> None:
    now = time.time()
    for jid in [j for j, v in JOBS.items() if now - v["created"] > JOB_TTL_SECONDS]:
        JOBS.pop(jid, None)


@app.post("/api/v1/audits", status_code=202)
async def create_audit_job(req: AuditRequest, x_llm_api_key: str | None = Header(default=None)) -> dict[str, Any]:
    state, config = _prepare(req, x_llm_api_key)
    with _credentials_file(req.cloud_credentials):
        pass
    _purge_jobs()
    job_id = uuid.uuid4().hex
    JOBS[job_id] = {"status": "running", "events": [], "result": None, "error": None, "created": time.time()}

    async def runner() -> None:
        job = JOBS[job_id]
        async for ev in _graph_events(req, state, config):
            if ev["event"] == "completed":
                job["result"], job["status"] = ev["result"], "completed"
            elif ev["event"] == "error":
                job["error"], job["status"] = ev["detail"], "error"
            else:
                job["events"].append(ev)

    task = asyncio.create_task(runner())
    _TASKS.add(task)
    task.add_done_callback(_TASKS.discard)
    return {"job_id": job_id, "nodes": NODE_SEQUENCE}


@app.get("/api/v1/audits/{job_id}")
async def get_audit_job(job_id: str) -> dict[str, Any]:
    job = JOBS.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Auditoría no encontrada (¿se reinició el backend?).")
    return {"job_id": job_id, "status": job["status"], "events": job["events"],
            "error": job["error"], "result": job["result"] if job["status"] == "completed" else None,
            "elapsed_s": round(time.time() - job["created"], 1)}


@app.get("/api/v1/reports/{report_id}.xlsx")
async def download_excel(report_id: str) -> FileResponse:
    path = report_path(report_id)
    if path is None:
        raise HTTPException(status_code=404, detail="Informe no encontrado o expirado (se conservan 24 h).")
    return FileResponse(path, filename=f"linaje_flujos_{report_id[:8]}.xlsx",
                        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
