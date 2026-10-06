"""Proveedor de descubrimiento SIMULADO (modo demo / tests: DISCOVERY_MODE=mock).

Simula la extracción de inventario desde la API del proveedor. Implementa la
interfaz `CloudDiscoveryProvider` para que en producción se sustituya por
colectores reales (boto3, google-cloud-asset, azure-mgmt-resourcegraph) sin
tocar el grafo (Dependency Inversion).
"""
from __future__ import annotations

import logging
import random
from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone
from typing import Any


logger = logging.getLogger(__name__)


class CloudDiscoveryProvider(ABC):
    @abstractmethod
    def discover(self, scope: str, target_id: str) -> dict[str, Any]: ...


class MockDiscoveryProvider(CloudDiscoveryProvider):
    """Genera un inventario realista y determinista (semilla = target_id)."""

    NAMING = {
        "AWS": {"bucket": "s3", "vm": "ec2", "disk": "ebs", "ip": "eip", "region": "us-east-1",
                "admin": "arn:aws:iam::aws:policy/AdministratorAccess", "read": "arn:aws:iam::aws:policy/ReadOnlyAccess",
                "types": ["t3.medium", "m5.xlarge", "c5.2xlarge", "r5.large"]},
        "GCP": {"bucket": "gcs", "vm": "gce", "disk": "pd", "ip": "address", "region": "us-central1",
                "admin": "roles/owner", "read": "roles/viewer",
                "types": ["e2-medium", "n2-standard-4", "c2-standard-8", "n2-highmem-2"]},
        "Azure": {"bucket": "blob", "vm": "vm", "disk": "manageddisk", "ip": "publicip", "region": "eastus",
                  "admin": "Owner", "read": "Reader",
                  "types": ["Standard_B2s", "Standard_D4s_v5", "Standard_F8s_v2", "Standard_E2s_v5"]},
    }

    def __init__(self, provider: str) -> None:
        if provider not in self.NAMING:
            raise ValueError(f"Proveedor cloud no soportado: {provider}")
        self.provider = provider
        self.n = self.NAMING[provider]

    def discover(self, scope: str, target_id: str) -> dict[str, Any]:
        rng = random.Random(f"{self.provider}:{scope}:{target_id}")
        now = datetime.now(timezone.utc)
        mult = {"organization": 3, "folder": 2, "project": 1}.get(scope, 1)
        envs = ["prod", "staging", "dev"]
        full_tags = lambda env, app: {"owner": "platform-team", "environment": env,  # noqa: E731
                                      "cost-center": "CC-1042", "application": app}

        def tags(env: str, app: str) -> dict[str, str]:
            t = full_tags(env, app)
            for k in list(t):
                if rng.random() < 0.3:
                    t.pop(k)
            return t

        iam_roles = []
        for i in range(6 * mult):
            kind = rng.choices(["admin", "wildcard", "scoped", "read"], [2, 2, 4, 3])[0]
            policy = {"admin": self.n["admin"], "read": self.n["read"],
                      "wildcard": {"actions": ["*"] if rng.random() < .5 else ["storage.*", "compute.*"],
                                   "resources": ["*"]},
                      "scoped": {"actions": ["storage.objects.get"], "resources": [f"{self.n['bucket']}-app-{i}/*"]}}[kind]
            iam_roles.append({
                "resource_id": f"iam-role-{i:03d}", "name": f"{rng.choice(['ci-deployer','legacy-admin','app-runtime','data-eng','support'])}-{i}",
                "attached_policies": [policy], "type": "service_account" if rng.random() < .6 else "user",
                "access_key_age_days": rng.choice([12, 45, 120, 400, 800]), "last_used_days_ago": rng.choice([1, 3, 60, 200]),
                "mfa_enabled": rng.random() < .6,
            })

        buckets = []
        for i in range(5 * mult):
            env = rng.choice(envs)
            enc = rng.choice(["NONE", "PROVIDER_MANAGED", "CMK"])
            buckets.append({
                "resource_id": f"{self.n['bucket']}-{env}-data-{i:03d}", "region": self.n["region"],
                "public_access": rng.random() < .25, "encryption": enc,
                "versioning": rng.random() < .5, "size_gb": rng.randint(1, 5000),
                "data_classification": rng.choice(["public", "internal", "confidential", "restricted"]),
                "tags": tags(env, f"app-{i % 4}"), "monthly_cost_usd": round(rng.uniform(2, 400), 2),
            })

        instances = []
        for i in range(6 * mult):
            env = rng.choice(envs)
            instances.append({
                "resource_id": f"{self.n['vm']}-{env}-{i:03d}", "machine_type": rng.choice(self.n["types"]),
                "state": "running", "public_ip": f"34.{rng.randint(1,254)}.{rng.randint(1,254)}.{rng.randint(1,254)}" if rng.random() < .35 else None,
                "firewall_ingress": [{"port": p, "source": rng.choice(["0.0.0.0/0", "10.0.0.0/8"])} for p in rng.sample([22, 80, 443, 3389, 5432], 2)],
                "avg_cpu_30d_pct": round(rng.choice([rng.uniform(0.5, 4.5), rng.uniform(15, 85)]), 1),
                "tags": tags(env, f"app-{i % 4}"), "monthly_cost_usd": round(rng.uniform(30, 900), 2),
            })

        volumes = []
        for i in range(7 * mult):
            attached = rng.random() < .65
            volumes.append({
                "resource_id": f"{self.n['disk']}-vol-{i:03d}", "size_gb": rng.choice([20, 100, 500, 1000]),
                "attached_to": rng.choice(instances)["resource_id"] if attached else None,
                "encrypted": rng.random() < .7, "tags": tags(rng.choice(envs), "storage"),
                "last_attached_days_ago": 0 if attached else rng.randint(20, 400),
                "monthly_cost_usd": round(rng.uniform(2, 120), 2),
            })

        ips = [{"resource_id": f"{self.n['ip']}-{i:03d}",
                "associated_with": rng.choice(instances)["resource_id"] if rng.random() < .5 else None,
                "monthly_cost_usd": 3.65} for i in range(3 * mult)]

        vol_ids = [v["resource_id"] for v in volumes]
        snapshots = [{"resource_id": f"snap-{i:04d}",
                      "source_volume": rng.choice(vol_ids + [f"{self.n['disk']}-vol-deleted-{i}"]),
                      "created_at": (now - timedelta(days=rng.randint(5, 900))).date().isoformat(),
                      "size_gb": rng.choice([20, 100, 500]), "monthly_cost_usd": round(rng.uniform(1, 25), 2)}
                     for i in range(4 * mult)]

        return {
            "metadata": {"provider": self.provider, "scope": scope, "target_id": target_id,
                         "collected_at": now.isoformat(), "collector": "mock-v1",
                         "required_tags": ["owner", "environment", "cost-center", "application"]},
            "iam_roles": iam_roles, "storage_buckets": buckets, "compute_instances": instances,
            "block_volumes": volumes, "static_ips": ips, "snapshots": snapshots,
        }
