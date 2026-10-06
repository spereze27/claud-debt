"""Precios de lista de GCP (USD, us-central1, on-demand) y estimadores de costo.

Son APROXIMACIONES para priorizar y comparar alternativas (p. ej. tamaño actual vs
recomendado). No sustituyen a la facturación real: si el usuario configura el export
de facturación a BigQuery, la UI y el informe muestran también el costo real.
"""
from __future__ import annotations

import math
import re
from dataclasses import dataclass

HOURS_PER_MONTH = 730.0
TIB = 1024 ** 4
GIB = 1024 ** 3

# (USD por vCPU-hora, USD por GB-hora) por familia de máquina Compute Engine.
FAMILY_PRICES: dict[str, tuple[float, float]] = {
    "e2": (0.021811, 0.002923),
    "n1": (0.031611, 0.004237),
    "n2": (0.031611, 0.004237),
    "n4": (0.030000, 0.004000),
    "n2d": (0.027502, 0.003686),
    "t2d": (0.027502, 0.003686),
    "t2a": (0.023100, 0.002900),
    "c2": (0.033980, 0.004550),
    "c2d": (0.029563, 0.003959),
    "c3": (0.034650, 0.003938),
    "c3d": (0.029760, 0.003990),
    "c4": (0.034800, 0.004000),
    "m1": (0.034800, 0.005100),
    "m2": (0.034800, 0.005100),
    "m3": (0.035000, 0.004700),
    "a2": (0.031611, 0.004237),
    "g2": (0.031611, 0.004237),
}
# GB de memoria por vCPU según tipo predefinido.
MEM_PER_VCPU = {"standard": 4.0, "highmem": 8.0, "highcpu": 1.0, "megamem": 14.9, "ultramem": 24.0}
N1_MEM_PER_VCPU = {"standard": 3.75, "highmem": 6.5, "highcpu": 0.9}
SHARED_CORE = {  # vCPU efectivas (para precio) y memoria
    "e2-micro": (0.25, 1.0), "e2-small": (0.5, 2.0), "e2-medium": (1.0, 4.0),
    "f1-micro": (0.2, 0.6), "g1-small": (0.5, 1.7),
}
PREEMPTIBLE_DISCOUNT = 0.30          # Spot/preemptible ≈ 60-91% dto.; usamos 70% de ahorro conservador → paga 30%
DATAPROC_PREMIUM_VCPU_H = 0.010
DATAPROC_SERVERLESS_DCU_H = 0.060
DATAFLOW_BATCH = (0.056, 0.003557)   # vCPU-h, GB-h
DATAFLOW_STREAM = (0.069, 0.003557)
CLOUDSQL_VCPU_H, CLOUDSQL_GB_H = 0.0413, 0.0070
CLOUDSQL_SHARED = {"db-f1-micro": 0.0105, "db-g1-small": 0.0350}
CLOUD_RUN_VCPU_S, CLOUD_RUN_GIB_S = 0.000024, 0.0000025
GKE_CLUSTER_FEE_H = 0.10
BQ_ON_DEMAND_PER_TIB = 6.25
BQ_SLOT_HOUR = 0.04                  # Standard edition, pay-as-you-go
BQ_STORAGE = {"active_logical": 0.02, "long_term_logical": 0.01, "active_physical": 0.04, "long_term_physical": 0.02}
GCS_STORAGE = {"STANDARD": 0.020, "NEARLINE": 0.010, "COLDLINE": 0.004, "ARCHIVE": 0.0012,
               "MULTI_REGIONAL": 0.026, "REGIONAL": 0.020, "DURABLE_REDUCED_AVAILABILITY": 0.020}
PD_PRICE_GB = {"pd-standard": 0.04, "pd-balanced": 0.10, "pd-ssd": 0.17, "pd-extreme": 0.125, "hyperdisk-balanced": 0.08}
COMPOSER_ENV_MONTH = {"ENVIRONMENT_SIZE_SMALL": 400.0, "ENVIRONMENT_SIZE_MEDIUM": 750.0, "ENVIRONMENT_SIZE_LARGE": 1500.0}


@dataclass(frozen=True)
class MachineShape:
    name: str
    family: str
    vcpus: float
    memory_gb: float

    @property
    def hourly(self) -> float:
        cpu_p, mem_p = FAMILY_PRICES.get(self.family, FAMILY_PRICES["n2"])
        return self.vcpus * cpu_p + self.memory_gb * mem_p

    @property
    def monthly(self) -> float:
        return self.hourly * HOURS_PER_MONTH


def parse_machine_type(machine: str | None) -> MachineShape | None:
    """'n2-standard-8', 'e2-medium', 'custom-4-16384', 'n2-custom-6-24576', URLs completas…"""
    if not machine:
        return None
    m = machine.rstrip("/").rsplit("/", 1)[-1].lower()
    if m in SHARED_CORE:
        v, mem = SHARED_CORE[m]
        return MachineShape(m, m.split("-")[0], v, mem)
    cm = re.match(r"^(?:([a-z0-9]+)-)?custom-(\d+)-(\d+)(?:-ext)?$", m)
    if cm:
        return MachineShape(m, cm.group(1) or "n1", float(cm.group(2)), int(cm.group(3)) / 1024)
    pm = re.match(r"^([a-z0-9]+)-(standard|highmem|highcpu|megamem|ultramem)-(\d+)", m)
    if pm:
        fam, kind, n = pm.group(1), pm.group(2), int(pm.group(3))
        per = (N1_MEM_PER_VCPU if fam == "n1" else MEM_PER_VCPU).get(kind, 4.0)
        if fam == "c3" and kind == "highcpu":
            per = 2.0
        return MachineShape(m, fam, float(n), n * per)
    return None


def machine_with_vcpus(shape: MachineShape, vcpus: int) -> str | None:
    """Mismo tipo con otro nº de vCPU (sólo tamaños válidos típicos)."""
    valid = [2, 4, 8, 16, 32, 48, 64, 80, 96]
    target = next((v for v in valid if v >= vcpus), None)
    if target is None:
        return None
    pm = re.match(r"^([a-z0-9]+)-(standard|highmem|highcpu)-(\d+)$", shape.name)
    if pm:
        return f"{pm.group(1)}-{pm.group(2)}-{target}"
    cm = re.match(r"^(?:([a-z0-9]+)-)?custom-(\d+)-(\d+)$", shape.name)
    if cm:
        per_cpu_mb = int(cm.group(3)) / int(cm.group(2))
        prefix = f"{cm.group(1)}-" if cm.group(1) else ""
        return f"{prefix}custom-{target}-{int(target * per_cpu_mb // 256 * 256)}"
    return None


def cloudsql_tier_shape(tier: str | None) -> tuple[float, float] | None:
    """Devuelve (vCPU, GB) de un tier de Cloud SQL."""
    if not tier:
        return None
    t = tier.lower()
    if t in CLOUDSQL_SHARED:
        return (0.0, 0.6 if t == "db-f1-micro" else 1.7)
    m = re.match(r"^db-custom-(\d+)-(\d+)$", t)
    if m:
        return float(m.group(1)), int(m.group(2)) / 1024
    m = re.match(r"^db-(?:n1|n2)-(standard|highmem)-(\d+)$", t)
    if m:
        n = int(m.group(2))
        return float(n), n * (3.75 if m.group(1) == "standard" else 6.5)
    m = re.match(r"^db-perf-optimized-n-(\d+)$", t)
    if m:
        n = int(m.group(1))
        return float(n), n * 8.0
    return None


def cloudsql_monthly(tier: str | None, high_availability: bool = False, disk_gb: float = 0,
                     disk_type: str = "PD_SSD") -> float | None:
    t = (tier or "").lower()
    if t in CLOUDSQL_SHARED:
        base = CLOUDSQL_SHARED[t] * HOURS_PER_MONTH
    else:
        shape = cloudsql_tier_shape(tier)
        if not shape:
            return None
        base = (shape[0] * CLOUDSQL_VCPU_H + shape[1] * CLOUDSQL_GB_H) * HOURS_PER_MONTH
    base *= 2 if high_availability else 1
    disk = disk_gb * (0.17 if "SSD" in (disk_type or "").upper() else 0.09) * (2 if high_availability else 1)
    return round(base + disk, 2)


def cloudsql_tier_for(vcpus: int, mem_per_vcpu: float) -> str:
    valid = [1, 2, 4, 8, 16, 32, 64, 96]
    v = next((x for x in valid if x >= max(vcpus, 1)), 96)
    mem_mb = int(max(3840, v * mem_per_vcpu * 1024) // 256 * 256)
    return f"db-custom-{v}-{mem_mb}"


def bq_query_cost(bytes_billed: float | int | None, slot_ms: float | int | None = None) -> float:
    """On-demand por bytes; si no hay bytes facturados pero sí slots (reservas/ediciones) → por slot-hora."""
    b = float(bytes_billed or 0)
    if b > 0:
        return b / TIB * BQ_ON_DEMAND_PER_TIB
    return float(slot_ms or 0) / 3.6e6 * BQ_SLOT_HOUR


def bq_storage_monthly(active_logical: float, lt_logical: float, active_physical: float, lt_physical: float,
                       ) -> dict[str, float]:
    logical = active_logical / GIB * BQ_STORAGE["active_logical"] + lt_logical / GIB * BQ_STORAGE["long_term_logical"]
    physical = active_physical / GIB * BQ_STORAGE["active_physical"] + lt_physical / GIB * BQ_STORAGE["long_term_physical"]
    return {"logical": round(logical, 2), "physical": round(physical, 2)}


def gcs_monthly(size_bytes: float, storage_class: str | None) -> float:
    return round(size_bytes / GIB * GCS_STORAGE.get((storage_class or "STANDARD").upper(), 0.02), 2)


def cloud_run_cost(billable_seconds: float, cpu: float = 1.0, mem_gib: float = 0.5) -> float:
    return billable_seconds * (cpu * CLOUD_RUN_VCPU_S + mem_gib * CLOUD_RUN_GIB_S)


def parse_cloud_run_resources(limits: dict[str, str] | None) -> tuple[float, float]:
    """{'cpu': '1000m', 'memory': '512Mi'} → (1.0, 0.5)"""
    limits = limits or {}
    cpu_s, mem_s = str(limits.get("cpu", "1")), str(limits.get("memory", "512Mi"))
    cpu = float(cpu_s[:-1]) / 1000 if cpu_s.endswith("m") else float(cpu_s or 1)
    m = re.match(r"^([\d.]+)\s*([KMGT]i?)?", mem_s)
    mult = {"Ki": 1 / 1024 ** 2, "Mi": 1 / 1024, "Gi": 1, "Ti": 1024, "K": 1e3 / GIB, "M": 1e6 / GIB, "G": 1e9 / GIB}
    mem = float(m.group(1)) * mult.get(m.group(2) or "Mi", 1 / 1024) if m else 0.5
    return cpu, mem


def dataflow_cost(vcpu_seconds: float, mem_gb_seconds: float, streaming: bool) -> float:
    cpu_p, mem_p = DATAFLOW_STREAM if streaming else DATAFLOW_BATCH
    return vcpu_seconds / 3600 * cpu_p + mem_gb_seconds / 3600 * mem_p


def node_group_monthly(machine: str | None, count: int, preemptible: bool = False, dataproc: bool = False,
                       hours: float = HOURS_PER_MONTH, disk_gb: float = 0, disk_type: str = "pd-standard") -> float:
    shape = parse_machine_type(machine)
    if not shape or count <= 0:
        return 0.0
    vm = shape.hourly * (PREEMPTIBLE_DISCOUNT if preemptible else 1.0)
    premium = shape.vcpus * DATAPROC_PREMIUM_VCPU_H if dataproc else 0.0
    disk = disk_gb * PD_PRICE_GB.get(disk_type, 0.04) / HOURS_PER_MONTH
    return round((vm + premium + disk) * count * hours, 2)


def ceil_int(x: float) -> int:
    return int(math.ceil(x - 1e-9))
