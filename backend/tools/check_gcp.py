"""Diagnóstico del acceso a GCP, sección por sección.

Uso (desde la raíz del proyecto):
    python -m backend.tools.check_gcp ruta/credenciales.json PROJECT_ID
"""
from __future__ import annotations

import json
import sys
import traceback

from backend.agent_graph.nodes.discovery import GCPDiscoveryProvider, _describe_error


def main(cred_path: str, project_id: str) -> int:
    with open(cred_path, encoding="utf-8") as fh:
        info = json.load(fh)
    print(f"Service Account : {info.get('client_email')}")
    print(f"Proyecto del JSON: {info.get('project_id')}")
    print(f"Proyecto auditado: {project_id}")
    if info.get("project_id") != project_id:
        print("  ⚠️  Son distintos: la SA necesita roles en el proyecto auditado.")
    p = GCPDiscoveryProvider(cred_path, project_id)
    checks = [("iam", p._collect_iam), ("storage", p._collect_buckets),
              ("compute_firewalls", p._collect_firewalls), ("compute_disks", p._collect_disks),
              ("compute_addresses", p._collect_addresses), ("compute_snapshots", p._collect_snapshots)]
    failed = 0
    for name, fn in checks:
        try:
            items = fn()
            print(f"✅ {name:18} {len(items)} recursos"
                  + (f"  → {[i['resource_id'] for i in items][:5]}" if items else ""))
        except Exception as exc:  # noqa: BLE001
            failed += 1
            print(f"❌ {name:18} {_describe_error(name, exc)}")
            print("   " + traceback.format_exception_only(type(exc), exc)[-1].strip()[:400])
    return 1 if failed else 0


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print(__doc__)
        sys.exit(2)
    sys.exit(main(sys.argv[1], sys.argv[2]))
