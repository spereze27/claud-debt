# Cloud Tech Debt Analyzer: cómo funciona el agente (v1.4)

## 1. Vista general

```mermaid
flowchart LR
    U([👤 Usuario]) --> FE[Streamlit<br/>frontend/app.py]
    FE -- "POST /api/v1/audits<br/>cabecera X-LLM-API-Key<br/>body: proyecto, alcance, JSON de la SA, opciones" --> API[FastAPI<br/>backend/main.py]
    API -- "202 job_id" --> FE
    FE -- "GET /api/v1/audits/{job_id}<br/>cada 2 s" --> API
    API -- "tarea en segundo plano" --> G[[Grafo LangGraph]]
    G -- "SDKs oficiales (solo lectura)" --> GCP[(Google Cloud)]
    G -- "prompts + datos" --> LLM{{LLM<br/>OpenAI / Anthropic / Gemini}}
    G -- ".xlsx" --> R[(Carpeta de informes<br/>se conservan 24 h)]
    FE -- "GET /api/v1/reports/{id}.xlsx" --> API
```

- La **API Key del LLM** viaja en una cabecera y se inyecta en `config["configurable"]`. Nunca entra en el estado, los logs ni las respuestas.
- El **JSON de la Service Account** se escribe en un archivo temporal con permisos `0600` y se borra al terminar. En el estado solo queda su ruta.

## 2. Grafo de agentes (LangGraph)

```mermaid
flowchart TD
    S((START)) --> D[🔎 discovery<br/><i>Python</i>]
    D --> A1[🛡️ security_analyzer<br/><i>LLM</i>]
    D --> A2[📋 governance_analyzer<br/><i>LLM</i>]
    D --> A3[💰 cost_analyzer<br/><i>LLM</i>]
    D --> A4[🗄️ data_governance_analyzer<br/><i>LLM + reglas</i>]
    D --> A5[🔄 pipeline_analyzer<br/><i>LLM + reglas</i>]
    D --> A6[🖥️ capacity_analyzer<br/><i>LLM + reglas</i>]
    A1 & A2 & A3 & A4 & A5 & A6 --> K[📊 kpi_calculator<br/><i>Python determinista</i>]
    K --> R1[📝 report_generator<br/><i>LLM</i>]
    K --> R2[📚 data_report_generator<br/><i>LLM + anexo Python</i>]
    R1 & R2 --> X[📗 excel_exporter<br/><i>pandas</i>]
    X --> E((END))
```

| Fase | Nodos | Qué hace |
|---|---|---|
| Descubrimiento | `discovery` | Lee GCP y construye `raw_infra_data`. No usa LLM. |
| Análisis en paralelo (fan-out) | 6 analizadores | Cada uno recibe solo su parte del inventario y devuelve `findings`. |
| Unión (fan-in) | `kpi_calculator` | Calcula índices de deuda (0-100), grado A-E y KPIs por dominio. |
| Informes en paralelo | `report_generator`, `data_report_generator` | Informe ejecutivo e informe de datos en Markdown. |
| Exportación | `excel_exporter` | Excel con inventario, dependencias, linaje, cascadas y hallazgos. |

## 3. Estado compartido (`AuditState`)

```mermaid
classDiagram
    class AuditState {
        provider, scope, target_id
        credentials_path  (temporal)
        billing_export_table
        data_options  (history_days, sample_lake, read_dags, max_projects, catalog)
        discovery_status  (complete | partial | failed | mock)
        raw_infra_data
        findings  ⟵ reducer operator.add
        errors    ⟵ reducer operator.add
        kpis
        final_report
        data_report
        excel_report
    }
```

`findings` y `errors` usan el reducer `operator.add`, así los seis analizadores escriben en paralelo sin pisarse.

## 4. Descubrimiento (`discovery`)

```mermaid
flowchart TD
    IN[Proyecto / Carpeta / Organización<br/>+ JSON de la Service Account] --> SC{¿Alcance?}
    SC -- Proyecto --> P1[1 proyecto]
    SC -- "Carpeta / Org" --> LP[Resource Manager:<br/>lista proyectos y subcarpetas] --> PN[N proyectos en paralelo<br/>máx. 4 a la vez]
    P1 & PN --> C

    subgraph C[Por proyecto: cada colector es independiente]
        direction TB
        B[Inventario base<br/>IAM · buckets · VMs · discos · IPs · snapshots · firewalls]
        AR[Arquitectura<br/>Cloud Asset Inventory → grafo de servicios<br/>quién habla con quién, con qué cuenta de servicio y permisos]
        BI[Facturación<br/>cuenta · presupuestos · costo real opcional]
        DP[Plataforma de datos<br/>BigQuery · Data Transfer · Dataflow · Dataproc<br/>Composer DAGs · Datastream · GCS · Monitoring]
    end

    C --> M[merge_projects<br/>ids prefijados por proyecto]
    M --> RAW[(raw_infra_data)]
    C -. "PermissionDenied en una sección" .-> W[aviso con el rol exacto que falta<br/>esa sección queda vacía, el resto sigue]
```

## 5. Motor de plataforma de datos (determinista)

```mermaid
flowchart LR
    subgraph Fuentes
        J[INFORMATION_SCHEMA.JOBS<br/>destino · tablas leídas · labels<br/>parent_job_id · bytes · duración]
        T[Data Transfer<br/>params, SQL, display_name]
        V[INFORMATION_SCHEMA.VIEWS]
        DF[Dataflow<br/>job_metadata · pasos Write]
        DPR[Dataproc<br/>args hdfs:// gs://]
        CO[DAGs de Composer<br/>código Python]
        DS[Datastream]
        CAT[Catálogo YAML<br/>flujos externos]
        SCH[Esquemas y etiquetas<br/>de tablas, datasets, buckets]
    end
    J & T & V & DF & DPR & CO & DS & CAT --> FB[FlowBuilder<br/>un flujo = pipeline, job o microservicio]
    FB --> IO[Entradas / salidas normalizadas<br/>+ modo: append · overwrite · merge]
    SCH --> L
    IO --> L[lineage.py<br/>capas · grupos · orden · dependencias]
    L --> IS[detect_issues<br/>conflictos · ciclos · inactivos · lentos<br/>saltos de capa · bronze sobrescrito]
    SCH --> TB[assess_table / assess_bucket<br/>capa · normalización · tipos · PII · formatos]
    CL[Clusters + uso p95] --> RS[rightsizing.py<br/>sobre/subdimensionado + costo]
    IS & TB & RS --> OUT[(data_platform<br/>flows · dependencies · cascades · tables<br/>lake · clusters · issues · cost_breakdown)]
```

### 5.1 Cómo obtiene el input y el output de un flujo

| Motor | Entradas | Salidas | Modo de escritura |
|---|---|---|---|
| Job de BigQuery | `referenced_tables` | `destination_table` | `statement_type` / `write_disposition` |
| Carga (LOAD) | `source_uris` | `destination_table` | `write_disposition` |
| Consulta programada / vista | `FROM` / `JOIN` del SQL | `INSERT` / `MERGE` / `CREATE` del SQL | según el verbo SQL |
| Transferencia GCS→BQ | `data_path_template` | `destination_table_name_template` | `APPEND` / `MIRROR` |
| Dataflow | `job_metadata` | pasos `Write*` / `Sink` | streaming = append |
| Dataproc | args `--input` | args `--output` | `overwrite` / `append` en args |
| Composer | operadores (bucket, conexión) | `destination_*_table` | `write_disposition` |
| Catálogo | declarado | declarado | declarado |

Si algo no se detecta, queda `no_detectada` y se genera el hallazgo `LINEAGE_UNDETECTED`.

### 5.2 Cómo infiere la capa

```mermaid
flowchart LR
    A[Dato] --> B{¿Etiqueta capa / layer?}
    B -- sí --> OK[capa de la etiqueta]
    B -- no --> C{¿Sistema origen?<br/>Postgres, HDFS, Kafka…}
    C -- sí --> SRC[source]
    C -- no --> D{¿Regex sobre nombre<br/>o contenedor?}
    D -- "landing / raw / stg / trusted / mart…" --> OK2[landing · bronze · silver · gold]
    D -- no --> ND[no_detectada]
```

La capa de un flujo es la más alta entre sus entradas (`capa_entrada_inferida`) y entre sus salidas (`capa_salida_inferida`). Las etiquetas del propio job (`capa_entrada`, `capa_salida`) tienen prioridad.

### 5.3 Cómo agrupa en cascadas

```mermaid
flowchart TD
    F[Flujo] --> P1{1. ¿Etiqueta grupo_cascada<br/>en el job? @@query_label}
    P1 -- sí --> G[grupo]
    P1 -- no --> P2{2. ¿Prefijo '[grupo]'<br/>en el nombre?}
    P2 -- sí --> G
    P2 -- no --> P3{3. ¿Etiqueta en las tablas o<br/>buckets que escribe / lee?}
    P3 -- sí --> G
    P3 -- no --> P4{4. ¿Conectado por linaje a<br/>flujos de un único grupo?}
    P4 -- sí --> G2[hereda ese grupo]
    P4 -- no --> P5[5. componente conexo<br/>grupo auto-xxxx]
    G & G2 & P5 --> O[orden_en_cascada<br/>profundidad en el grafo de dependencias]
```

Hay una dependencia **A → B** cuando una salida de A es entrada de B.

## 6. Analizadores (patrón común)

```mermaid
flowchart LR
    RAW[(raw_infra_data)] --> SD[select_data<br/>solo su parte]
    SD --> LLM{{LLM con salida estructurada}}
    LLM --> GR[Guardrail anti-alucinación<br/>regla del catálogo de su categoría<br/>+ resource_id existente]
    RAW --> DET[Hallazgos deterministas<br/>data_findings.py]
    GR & DET --> MRG[Unión<br/>gana el del LLM si coincide regla + recurso]
    MRG --> FND[findings]
    LLM -. "si falla" .-> DET
```

| Analizador | Evalúa |
|---|---|
| Seguridad | mínimo privilegio, servicios públicos, cuentas de servicio por defecto, secretos en variables de entorno, bases de datos expuestas |
| Gobierno | etiquetas, cifrado, backups |
| Costos | recursos huérfanos u ociosos, presupuestos, servicios siempre encendidos |
| Gobierno de datos | capa correcta, normalización, tipos, PII, particionado, data lake, HDFS |
| Flujos | conflictos, ciclos, inactividad, fallos, lentitud, escaneos completos, cascadas sin grupo o con varios dueños |
| Capacidad | clusters Dataproc, GKE, Cloud SQL y Composer frente al uso p95, con costo actual y recomendado |

## 7. Salidas

```mermaid
flowchart LR
    ST[(Estado final)] --> UI[Interfaz: 11 pestañas<br/>KPIs · Arquitectura · Flujos · Datos · Capacidad<br/>Identidades · Facturación · Informe · Informe de datos<br/>Hallazgos · Inventario]
    ST --> MD1[Informe ejecutivo .md]
    ST --> MD2[Informe de datos .md<br/>narrativa LLM + anexo con cifras exactas]
    ST --> XL[Excel<br/>00_Resumen · 01_Inventario_Flujos<br/>02_Dependencias_Flujos · 03_Lineaje<br/>04_Grupos_Cascada · 05_Hallazgos]
```

## 8. Principios de diseño

- **El LLM no calcula cifras.** Precios, KPIs, dimensionamiento y linaje salen de Python; el LLM los explica y prioriza.
- **Resiliencia.** Un permiso faltante, un proyecto roto o un LLM caído no detienen la auditoría: se registra el aviso y se continúa.
- **Solo lectura.** Las consultas a `INFORMATION_SCHEMA` tienen un tope de bytes facturados y el agente excluye sus propios jobs del análisis.
- **Seguridad de salida.** El Excel neutraliza fórmulas y caracteres de control. Los diagramas Mermaid generados por el LLM se dibujan en modo `strict`.