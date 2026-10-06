"""Nodo final Excel_Exporter: escribe el linaje y los hallazgos del estado en un .xlsx."""
from __future__ import annotations

import logging
import os
import re
import tempfile
import time
import uuid
from typing import Any

from backend.agent_graph.state import AuditState
from backend.core.excel_report import build_frames, write_excel

logger = logging.getLogger(__name__)
REPORTS_DIR = os.getenv("REPORTS_DIR") or os.path.join(tempfile.gettempdir(), "cloud_tech_debt_reports")
REPORT_TTL_SECONDS = 24 * 3600
REPORT_ID_RE = re.compile(r"^[0-9a-f]{32}$")


def report_path(report_id: str) -> str | None:
    """Ruta del informe si el id es válido y existe (protege contra path traversal)."""
    if not REPORT_ID_RE.match(report_id or ""):
        return None
    path = os.path.join(REPORTS_DIR, f"{report_id}.xlsx")
    return path if os.path.isfile(path) else None


def _purge_old() -> None:
    try:
        now = time.time()
        for name in os.listdir(REPORTS_DIR):
            p = os.path.join(REPORTS_DIR, name)
            if name.endswith(".xlsx") and now - os.path.getmtime(p) > REPORT_TTL_SECONDS:
                os.remove(p)
    except OSError:
        pass


def excel_export_node(state: AuditState) -> dict[str, Any]:
    try:
        os.makedirs(REPORTS_DIR, mode=0o700, exist_ok=True)
        _purge_old()
        frames = build_frames(dict(state))
        report_id = uuid.uuid4().hex
        path = os.path.join(REPORTS_DIR, f"{report_id}.xlsx")
        write_excel(frames, path)
        safe_target = re.sub(r"[^A-Za-z0-9_\-]", "_", state.get("target_id") or "auditoria")[:60]
        return {"excel_report": {"report_id": report_id, "path": path,
                                 "filename": f"linaje_flujos_{safe_target}.xlsx",
                                 "sheets": {name: len(df) for name, df in frames.items()}}}
    except Exception as exc:  # noqa: BLE001 — la exportación nunca debe tumbar la auditoría
        logger.exception("Exportación a Excel falló")
        return {"excel_report": None, "errors": [f"excel_export: {type(exc).__name__}: {exc}"]}
