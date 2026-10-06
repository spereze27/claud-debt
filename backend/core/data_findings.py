"""Hallazgos DETERMINISTAS a partir de las señales del motor de datos.

Garantizan que cada problema detectado por reglas llegue al informe aunque el LLM lo
omita. Los analizadores LLM reciben las mismas señales y aportan juicio experto y
recomendaciones más ricas; si generan un hallazgo con el mismo (regla, recurso), el
suyo tiene prioridad sobre el de esta plantilla.
"""
from __future__ import annotations

from typing import Any

RECOMMENDATIONS = {
    "LAYER_UNDECLARED": "Declarar la capa con la etiqueta `layer=bronze|silver|gold` y una convención de nombres por dataset.",
    "LAYER_MISMATCH": "Mover la transformación a la capa que corresponde: bronze = copia fiel del origen; "
                      "joins y limpieza en silver; agregaciones de negocio en gold.",
    "LAYER_SKIP": "Aterrizar primero en bronze (append-only, sin transformar) y construir silver/gold desde ahí.",
    "LAYER_INVERSION": "Invertir la dependencia: las capas inferiores nunca deben leer de las superiores. "
                       "Si se necesita el dato, recalcularlo desde silver.",
    "BRONZE_OVERWRITE": "Cambiar a WRITE_APPEND / INSERT particionado por fecha de ingesta y conservar el histórico crudo.",
    "NORMALIZATION_ISSUE": "Silver: separar entidades (dimensiones) y referenciarlas por clave. Gold: publicar tablas "
                           "anchas o modelo estrella listas para consumo.",
    "SCHEMA_TYPE_ISSUE": "Convertir a DATE/TIMESTAMP/NUMERIC en silver (SAFE_CAST) y rechazar filas inválidas a una tabla de cuarentena.",
    "NO_PARTITIONING": "Particionar por la columna de fecha de negocio o de ingesta y exigir filtro de partición "
                       "(require_partition_filter).",
    "NO_CLUSTERING": "Agregar clustering por las columnas más filtradas/uniones (hasta 4).",
    "PII_COLUMNS": "Aplicar policy tags de Data Catalog, enmascaramiento dinámico y acceso por roles; evitar PII en gold si no es necesaria.",
    "UNDOCUMENTED": "Documentar descripción, dueño y significado de columnas (Dataplex / Data Catalog).",
    "WRONG_FILE_FORMAT": "Convertir a Parquet/Avro (columnar, comprimido, con esquema) desde silver en adelante.",
    "SMALL_FILES": "Compactar a archivos de 128–512 MB (job de compactación o ajustar el paralelismo de escritura).",
    "WRONG_STORAGE_CLASS": "Aplicar reglas de ciclo de vida (Nearline a 30 días, Coldline a 90) o Autoclass según el patrón de acceso.",
    "NO_LIFECYCLE": "Definir retención/borrado y transición de clase en zonas de aterrizaje y temporales.",
    "NO_PARTITION_LAYOUT": "Escribir con rutas estilo Hive (dt=AAAA-MM-DD/) para lecturas incrementales y tablas externas particionadas.",
    "MIXED_LAYERS": "Separar un bucket por capa (bronze/silver/gold) para permisos, ciclo de vida y costos independientes.",
    "STORAGE_BILLING_MODEL": "Cambiar el modelo de facturación de almacenamiento del dataset (ALTER SCHEMA … SET OPTIONS "
                             "(storage_billing_model = …)).",
    "HDFS_PERSISTENT_STORAGE": "Mover los datos de HDFS a Cloud Storage (conector GCS) para poder usar clusters efímeros.",
    "FLOW_CONFLICT": "Definir un único dueño por tabla destino; si varios procesos deben escribir, usar MERGE por clave "
                     "o tablas separadas unidas por una vista.",
    "FLOW_CYCLE": "Romper el ciclo: materializar un paso intermedio y ordenar las dependencias en un DAG.",
    "FLOW_INACTIVE": "Confirmar con el dueño y desactivar/eliminar el flujo y sus recursos, o reactivarlo si es necesario.",
    "FLOW_FAILING": "Revisar el error de las últimas ejecuciones, añadir reintentos y alertas (Cloud Monitoring).",
    "SLOW_FLOW": "Procesamiento incremental, particionado/clustering de entradas y paralelizar pasos independientes.",
    "FULL_SCAN": "Filtrar por la columna de partición (WHERE fecha >= …) y seleccionar sólo las columnas necesarias.",
    "FULL_RECOMPUTE": "Procesar sólo las particiones nuevas/modificadas con MERGE o INSERT OVERWRITE por partición.",
    "SELECT_STAR": "Listar explícitamente las columnas necesarias.",
    "REDUNDANT_FLOW": "Unificar la lógica en un solo flujo y reutilizar su resultado.",
    "ORPHAN_OUTPUT": "Verificar si alguien consume la tabla; si no, dejar de producirla o archivarla.",
    "FLOW_UNGROUPED": "Etiquetar el flujo con grupo_cascada (SET @@query_label en scripts, labels en jobs/recursos) "
                      "o usar la convención '[grupo] nombre' en el display_name.",
    "CASCADE_MULTI_OWNER": "Definir un dueño único (etiqueta owner) para la cascada completa y un acuerdo de servicio entre equipos.",
    "LINEAGE_UNDETECTED": "Registrar entradas y salidas del flujo en el catálogo de flujos o en Dataplex Lineage.",
    "CLUSTER_OVERSIZED": "Reducir la capacidad según la recomendación y habilitar autoscaling.",
    "CLUSTER_UNDERSIZED": "Aumentar la capacidad o habilitar autoscaling con un máximo mayor.",
    "CLUSTER_NO_AUTOSCALING": "Habilitar autoscaling (política YARN en Dataproc, cluster autoscaler en GKE).",
    "CLUSTER_ALWAYS_ON": "Usar clusters efímeros por job (o Dataproc Serverless) con idle-delete TTL.",
    "DB_OVERSIZED": "Bajar el tier de la instancia y revisar conexiones/consultas pesadas.",
    "DB_UNDERSIZED": "Subir el tier o agregar réplicas de lectura; revisar índices y consultas lentas.",
    "COMPOSER_OVERSIZED": "Reducir el tamaño del entorno o migrar a Workflows/Dataform si hay pocos DAGs.",
}

TITLES = {
    "LAYER_UNDECLARED": "Tabla sin capa declarada", "LAYER_MISMATCH": "Transformación en la capa equivocada",
    "LAYER_SKIP": "Flujo que se salta una capa", "LAYER_INVERSION": "Flujo en sentido inverso entre capas",
    "BRONZE_OVERWRITE": "Bronze sobrescrito", "NORMALIZATION_ISSUE": "Nivel de normalización no acorde a la capa",
    "SCHEMA_TYPE_ISSUE": "Tipos de datos incorrectos", "NO_PARTITIONING": "Tabla grande sin particionar",
    "NO_CLUSTERING": "Tabla grande sin clustering", "PII_COLUMNS": "Posibles datos personales sin gobierno",
    "UNDOCUMENTED": "Tabla sin documentación", "WRONG_FILE_FORMAT": "Formato de archivo inadecuado",
    "SMALL_FILES": "Problema de archivos pequeños", "WRONG_STORAGE_CLASS": "Clase de almacenamiento inadecuada",
    "NO_LIFECYCLE": "Sin reglas de ciclo de vida", "NO_PARTITION_LAYOUT": "Rutas del lake sin particionado",
    "MIXED_LAYERS": "Bucket con capas mezcladas", "STORAGE_BILLING_MODEL": "Modelo de facturación de almacenamiento mejorable",
    "HDFS_PERSISTENT_STORAGE": "Datos en HDFS de un cluster persistente",
    "FLOW_CONFLICT": "Flujos en conflicto sobre el mismo destino", "FLOW_CYCLE": "Dependencia circular entre flujos",
    "FLOW_INACTIVE": "Flujo inactivo", "FLOW_FAILING": "Flujo con fallos", "SLOW_FLOW": "Flujo lento",
    "FULL_SCAN": "Escaneo completo de datos", "FULL_RECOMPUTE": "Recalcula todo en cada ejecución",
    "SELECT_STAR": "Uso de SELECT *", "REDUNDANT_FLOW": "Lógica duplicada entre flujos",
    "ORPHAN_OUTPUT": "Salida que nadie consume",
    "FLOW_UNGROUPED": "Flujo sin grupo declarado", "CASCADE_MULTI_OWNER": "Cascada con varios dueños",
    "LINEAGE_UNDETECTED": "Linaje no detectable",
    "CLUSTER_OVERSIZED": "Cluster sobredimensionado", "CLUSTER_UNDERSIZED": "Cluster subdimensionado",
    "CLUSTER_NO_AUTOSCALING": "Cluster sin autoscaling", "CLUSTER_ALWAYS_ON": "Cluster encendido permanentemente",
    "DB_OVERSIZED": "Base de datos sobredimensionada", "DB_UNDERSIZED": "Base de datos subdimensionada",
    "COMPOSER_OVERSIZED": "Entorno Composer sobredimensionado",
}

PIPELINE_CODES = {"FLOW_CONFLICT", "FLOW_CYCLE", "FLOW_INACTIVE", "FLOW_FAILING", "SLOW_FLOW", "FULL_SCAN",
                  "FULL_RECOMPUTE", "SELECT_STAR", "REDUNDANT_FLOW", "ORPHAN_OUTPUT", "FLOW_UNGROUPED",
                  "CASCADE_MULTI_OWNER", "LINEAGE_UNDETECTED"}


def _f(category: str, rule: str, rid: str, rtype: str, severity: str, text: str, waste: float = 0.0,
       recommendation: str | None = None) -> dict[str, Any]:
    return {"rule": rule, "resource_id": rid, "resource_type": rtype, "severity": severity,
            "title": TITLES.get(rule, rule), "description": text,
            "recommendation": recommendation or RECOMMENDATIONS.get(rule, ""),
            "estimated_monthly_waste_usd": round(float(waste or 0), 2), "category": category}


def data_governance_findings(dp: dict[str, Any]) -> list[dict[str, Any]]:
    out = []
    for t in dp.get("tables") or []:
        for s in t.get("signals") or []:
            if s["code"] in ("UNDOCUMENTED", "LAYER_UNDECLARED") and t["size_gb"] < 1:
                continue  # ruido en tablas pequeñas
            out.append(_f("data_governance", s["code"], t["resource_id"], "BigQuery table", s["severity"], s["text"]))
    for b in dp.get("lake") or []:
        for s in b.get("signals") or []:
            waste = 0.0
            if s["code"] == "WRONG_STORAGE_CLASS" and "ahorraría" in s["text"]:
                try:
                    waste = float(s["text"].split("~$")[1].split("/")[0].replace(",", ""))
                except (IndexError, ValueError):
                    waste = 0.0
            out.append(_f("data_governance", s["code"], b["resource_id"], "GCS bucket", s["severity"], s["text"], waste))
    for d in dp.get("datasets") or []:
        if d.get("billing_model_saving_usd"):
            out.append(_f("data_governance", "STORAGE_BILLING_MODEL", d["resource_id"], "BigQuery dataset", "LOW",
                          f"El dataset {d['dataset']} factura almacenamiento {d['storage_billing_model']}; con "
                          f"{d['recommended_billing_model']} ahorraría ~${d['billing_model_saving_usd']:,.2f}/mes.",
                          d["billing_model_saving_usd"]))
    for c in dp.get("clusters") or []:
        if c.get("hdfs_heavy"):
            out.append(_f("data_governance", "HDFS_PERSISTENT_STORAGE", c["resource_id"], "Dataproc cluster", "MEDIUM",
                          f"El cluster {c['name']} guarda datos en HDFS sobre discos persistentes/SSD locales."))
    for i in dp.get("issues") or []:
        if i["code"] not in PIPELINE_CODES:
            out.append(_f("data_governance", i["code"], i["resource_id"], "flujo", i["severity"], i["text"]))
    return out


def pipeline_findings(dp: dict[str, Any]) -> list[dict[str, Any]]:
    flows = {f["resource_id"]: f for f in dp.get("flows") or []}
    out = []
    for i in dp.get("issues") or []:
        if i["code"] not in PIPELINE_CODES:
            continue
        f = flows.get(i["resource_id"], {})
        cost = float(f.get("cost_30d_usd") or 0)
        waste = {"FLOW_INACTIVE": cost, "FULL_SCAN": cost * 0.7, "FULL_RECOMPUTE": cost * 0.5,
                 "REDUNDANT_FLOW": cost * 0.5, "ORPHAN_OUTPUT": cost}.get(i["code"], 0.0)
        out.append(_f("pipelines", i["code"], i["resource_id"], f.get("engine") or "flujo", i["severity"], i["text"], waste))
    return out


def capacity_findings(dp: dict[str, Any]) -> list[dict[str, Any]]:
    out = []
    for c in dp.get("clusters") or []:
        s = c.get("sizing") or {}
        kind, rid = c.get("kind"), c["resource_id"]
        prefix = {"cloudsql": "DB", "composer": "COMPOSER"}.get(kind, "CLUSTER")
        reasons = " ".join(s.get("reasons") or [])
        if s.get("verdict") == "oversized":
            out.append(_f("capacity", f"{prefix}_OVERSIZED" if prefix != "COMPOSER" else "COMPOSER_OVERSIZED", rid, kind,
                          "HIGH" if -(s.get("monthly_delta_usd") or 0) > 500 else "MEDIUM",
                          f"{reasons} {s.get('explanation') or ''} Configuración actual: {s.get('current_config')}; "
                          f"recomendada: {s.get('recommended_config')}.", -(s.get("monthly_delta_usd") or 0)))
        elif s.get("verdict") == "undersized" and prefix != "COMPOSER":
            out.append(_f("capacity", f"{prefix}_UNDERSIZED", rid, kind, "HIGH",
                          f"{reasons} {s.get('explanation') or ''} Recomendado: {s.get('recommended_config')}.", 0.0))
        for e in s.get("additional_savings") or []:
            rule = "CLUSTER_ALWAYS_ON" if "efímero" in e["action"] else "CLUSTER_NO_AUTOSCALING" \
                if "autoscaling" in e["action"].lower() else None
            if rule:
                out.append(_f("capacity", rule, rid, kind, "MEDIUM", f"{c['name']}: {e['action']}.", e["monthly_saving"],
                              e["action"]))
    return out


def all_rules() -> dict[str, str]:
    rules = {k: "data_governance" for k in TITLES if k not in PIPELINE_CODES}
    rules.update({k: "pipelines" for k in PIPELINE_CODES})
    rules.update({k: "capacity" for k in ("CLUSTER_OVERSIZED", "CLUSTER_UNDERSIZED", "CLUSTER_NO_AUTOSCALING",
                                          "CLUSTER_ALWAYS_ON", "DB_OVERSIZED", "DB_UNDERSIZED", "COMPOSER_OVERSIZED")})
    return rules
