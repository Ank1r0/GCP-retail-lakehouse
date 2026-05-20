# retail-lakehouse :: Master Project Map

One page that holds the **whole project** at once: the data pipeline from extract to gold,
the GCP tool behind every stage, **why** that tool was chosen, and how each stage maps to a
Jira ticket. The smaller per-topic diagrams in [`architecture.md`](architecture.md) still
stand — this file is the single bird's-eye view on top of them.

Status legend used everywhere below: ✅ done · 🟡 in progress · ⬜ to do

---

## 1. Master Pipeline Blueprint

The full system in one diagram — six data layers (the spine), plus the three cross-cutting
layers that wrap it: **planning** (Jira), **orchestration** (Scheduler + Composer), and
**foundation** (Terraform + GitHub Actions). Each layer's title carries its Jira Epic key.

```mermaid
flowchart TB
    classDef store fill:#fff3e0,stroke:#ef6c00,color:#4e342e
    classDef tool  fill:#e3f2fd,stroke:#1976d2,color:#0d47a1
    classDef plan  fill:#ede7f6,stroke:#5e35b1,color:#311b92
    classDef todo  fill:#eceff1,stroke:#90a4ae,color:#37474f

    subgraph PLAN["PLANNING — Jira project KAN · 6 Epics / 17 Tasks"]
        JIRA["Jira Cloud · ank1r0.atlassian.net<br/>Epic to Task to status · sets delivery order"]
    end

    subgraph ORCH["ORCHESTRATION — Epic KAN-8 · KAN-24 ⬜"]
        direction LR
        SCHED["Cloud Scheduler<br/>managed cron · daily 02:00 UTC"]
        COMP["Cloud Composer — Airflow<br/>DAG retail_daily<br/>dependencies · retries · sensors · backfill"]
        SCHED -->|fires| COMP
    end

    subgraph SRC["1 · SOURCE — OLTP database · ✅ done before Jira existed"]
        direction LR
        GEN["generate.py<br/>Faker — synthetic data, no real PII"]
        PG[("PostgreSQL · schema retail · 7 tables<br/>dev: Docker · prod: Cloud SQL")]
        GEN -->|seeds| PG
    end

    subgraph EXT["2 · EXTRACT — Epic KAN-4 · KAN-12 🟡"]
        direction LR
        EXJOB["extract_to_gcs.py<br/>incremental · watermarks · idempotent"]
        AR["Artifact Registry<br/>Docker image store"]
        EXRUN["Cloud Run Job · retail-extract<br/>serverless container · pay-per-run"]
        EXJOB -->|docker build + push| AR
        AR -->|image pulled by| EXRUN
    end

    subgraph BRZ["3 · BRONZE — raw landing · Epic KAN-4 · KAN-10 ✅ KAN-11 ✅"]
        direction LR
        GCS[("GCS · retail-lakehouse-dev<br/>bronze/retail__&lt;table&gt;/dt=date/run=*.csv.gz<br/>immutable archive · Hive-partitioned")]
        LOADJOB["load_to_bq.py<br/>Cloud Run Job · load_table_from_uri"]
        BQB[("BigQuery · bronze_dev<br/>raw 1:1 · WRITE_TRUNCATE · schema autodetect")]
        GCS -->|GCS to BQ inside Google network| LOADJOB --> BQB
    end

    subgraph SLV["4 · SILVER — clean · typed · historised · Epic KAN-5 ⬜"]
        direction LR
        SLVT["KAN-13 silver_customers cast<br/>KAN-14 orders·items·payments·suppliers<br/>KAN-15 SCD2 customers<br/>KAN-16 SCD2 products<br/>KAN-17 category hierarchy path"]
        SPARK["PySpark on Dataproc Serverless<br/>no cluster to manage"]
        BQS[("BigQuery · silver_dev<br/>deduped · typed · SCD2")]
        SLVT -.implemented by.-> SPARK
        SPARK --> BQS
    end

    subgraph GLD["5 · GOLD — business marts · Epic KAN-6 ⬜"]
        direction LR
        GLDT["KAN-18 scaffold + profiles<br/>KAN-19 staging models<br/>KAN-20 fct_orders · fct_revenue_daily<br/>KAN-21 customer_360 · ar_aging · margin<br/>KAN-22 tests + dbt docs"]
        DBT["dbt-bigquery · Cloud Run Job<br/>staging to intermediate to marts"]
        BQG[("BigQuery · gold_dev<br/>fct_ / dim_ / mart_ tables")]
        GLDT -.implemented by.-> DBT
        DBT --> BQG
    end

    subgraph BI["6 · SERVE — BI · Epic KAN-7 · KAN-23 ⬜"]
        MB["Metabase · 4 dashboards<br/>Executive · Customer · Operations · Finance<br/>reads gold only"]
    end

    subgraph INFRA["FOUNDATION — Epic KAN-9 ⬜"]
        direction LR
        TF["Terraform · KAN-25<br/>buckets · datasets · service accounts · IAM<br/>workspaces: dev / prod"]
        GHA["GitHub Actions · KAN-26<br/>PR lint+test · build image · deploy jobs+DAG"]
    end

    %% ---- data spine (thick) ----
    PG ==> EXRUN
    EXRUN ==> GCS
    BQB ==> SPARK
    BQS ==> DBT
    BQG ==> MB

    %% ---- planning governs delivery ----
    PLAN -.plans and tracks all layers.-> ORCH

    %% ---- orchestration triggers the jobs ----
    COMP -.triggers.-> EXRUN
    COMP -.triggers.-> LOADJOB
    COMP -.triggers.-> SPARK
    COMP -.triggers.-> DBT

    %% ---- foundation underpins everything ----
    TF -.provisions.-> BRZ
    GHA -.builds image into.-> AR

    class PG,GCS,BQB,BQS,BQG store
    class GEN,EXJOB,AR,EXRUN,LOADJOB,SPARK,DBT,MB,SCHED,COMP,TF,GHA tool
    class JIRA plan
    class SLVT,GLDT todo
```

**How to read it:** the thick arrows are the data spine — a row physically moves
Postgres → GCS → BigQuery bronze → silver → gold → Metabase. The dotted arrows are
*control*, not data: Composer **triggers** jobs, Terraform **provisions** the resources
those jobs write into, Jira **tracks** the work. Data never flows through the dotted lines.

---

## 2. Why Each Tool — and What Breaks Without It

The point of the project is to meet the GCP toolset. For every box above, here is the
job it does, why it (and not an alternative) was picked, and the consequence of removing it.

| Stage | Tool | Why this one | Remove it and… |
|---|---|---|---|
| Source | **Faker** (`generate.py`) | Generates realistic fake retail rows so the whole pipeline runs on **zero real PII** — safe for a public portfolio. | You'd need a real dataset: privacy risk, licensing, can't publish. |
| Source | **PostgreSQL** | A row-oriented, transactional DB — mirrors the real on-prem operational system a lakehouse must extract *from*. | No realistic source shape; you'd be "transforming" data that never lived in an OLTP system. |
| Source (prod) | **Cloud SQL** | Managed PostgreSQL — Google runs backups, patching, failover. | You self-manage a Postgres VM: patching, disk, HA all become your job. |
| Extract | **`extract_to_gcs.py`** (Python) | The extract logic is bespoke (watermarks, per-table modes) — a hand-written script teaches more and costs nothing. | A managed connector (Datastream, Fivetran) — paid, less control, less learning. |
| Bronze | **Cloud Storage (GCS)** | Cheap, immutable object store = the **archive**. Decouples extract from load, survives a BQ table drop, Hive-partitioned for cheap re-loads. | Extract straight into BQ — you lose the replayable archive and the extract/load split. |
| Extract/Bronze | **Artifact Registry** | Private, in-region, **versioned** store for the Docker images Cloud Run runs. | Cloud Run has no image to pull — nothing to deploy. |
| Extract/Bronze | **Cloud Run Jobs** | Serverless container, **run-to-completion, $0 when idle** — the exact shape of a finite batch job. | A VM on cron (idle cost + ops) or GKE (cluster to babysit). |
| Orchestration | **Cloud Scheduler** | Managed cron — the single timer that kicks off the pipeline. | A machine must stay powered on just to run `cron`. |
| Warehouse | **BigQuery** | Serverless columnar warehouse; storage and compute are separate; loads CSV from GCS **natively**; SQL at any scale. Home of bronze/silver/gold. | You'd size and tune your own warehouse cluster (vacuum, distribution keys, scaling). |
| Silver | **Dataproc Serverless** (PySpark) | Runs Spark with **no standing cluster**. SCD2, dedup via window functions, heavy joins — verbose in SQL, natural in Spark. | Do silver in pure SQL (SCD2 gets painful) or manage a Spark cluster yourself. |
| Gold | **dbt-bigquery** | SQL transformation framework: models, `ref()`, tests, docs, lineage — all version-controlled. The business-logic layer. | Loose hand-written SQL: no tests, no lineage graph, no generated docs. |
| Serve | **Metabase** | Open-source BI — point-and-click dashboards straight on BigQuery. | No serving layer; stakeholders hand-write SQL to answer questions. |
| Orchestration | **Cloud Composer (Airflow)** | Managed Airflow — DAG **dependencies, retries, sensors, backfills**, run history. Chains extract→load→spark→dbt with real failure handling. | A chain of schedulers with no dependency awareness — step 3 runs even when step 2 failed. |
| Foundation | **Terraform** | Infrastructure as code — every bucket/dataset/SA defined in files, applied **identically to dev and prod**. | Click-ops in the console: not reproducible, no review, dev/prod silently drift. |
| Foundation | **GitHub Actions** | CI/CD — on PR run lint+tests; on merge build the image and deploy jobs + DAG. | Manual `docker build` / `gcloud deploy` on every change (fine solo, doesn't scale). |
| Planning | **Jira** | Turns the architecture roadmap into trackable Epics and Tasks with status and order. | The roadmap lives only in a doc — no progress signal, no "what's next". |

---

## 3. Jira Delivery Map (Epic → Task)

The same project seen as **planned work**. 6 Epics, one per pipeline layer; 18 Tasks beneath
them — including **KAN-27**, created to close the monitoring gap (see §4).

```mermaid
flowchart LR
    classDef done fill:#c8e6c9,stroke:#2e7d32,color:#1b5e20
    classDef prog fill:#fff9c4,stroke:#f9a825,color:#5f4300
    classDef todo fill:#eceff1,stroke:#90a4ae,color:#37474f
    classDef epic fill:#ede7f6,stroke:#5e35b1,color:#311b92

    ROOT["retail-lakehouse<br/>Jira project KAN"]:::epic

    ROOT --> E4["KAN-4 · Bronze Layer"]:::epic
    ROOT --> E5["KAN-5 · Silver Layer"]:::epic
    ROOT --> E6["KAN-6 · Gold Layer"]:::epic
    ROOT --> E7["KAN-7 · Presentation"]:::epic
    ROOT --> E8["KAN-8 · Orchestration"]:::epic
    ROOT --> E9["KAN-9 · Infra and CI/CD"]:::epic

    E4 --> K10["KAN-10 rename dataset to bronze_dev ✅"]:::done
    E4 --> K11["KAN-11 load 7 tables to bronze_dev ✅"]:::done
    E4 --> K12["KAN-12 dockerize + Cloud Run Job 🟡"]:::prog

    E5 --> K13["KAN-13 silver_customers cast"]:::todo
    E5 --> K14["KAN-14 orders/items/payments/suppliers"]:::todo
    E5 --> K15["KAN-15 SCD2 customers"]:::todo
    E5 --> K16["KAN-16 SCD2 products"]:::todo
    E5 --> K17["KAN-17 category hierarchy path"]:::todo

    E6 --> K18["KAN-18 dbt scaffold + profiles"]:::todo
    E6 --> K19["KAN-19 dbt staging models"]:::todo
    E6 --> K20["KAN-20 fct_orders + fct_revenue_daily"]:::todo
    E6 --> K21["KAN-21 customer_360 + ar_aging + margin"]:::todo
    E6 --> K22["KAN-22 dbt tests + docs"]:::todo

    E7 --> K23["KAN-23 Metabase + 4 dashboards"]:::todo

    E8 --> K24["KAN-24 Airflow DAG retail_daily"]:::todo
    E8 --> K27["KAN-27 Cloud Monitoring alerts<br/>+ log-based metrics"]:::todo

    E9 --> K25["KAN-25 Terraform dev + prod"]:::todo
    E9 --> K26["KAN-26 GitHub Actions CI/CD"]:::todo
```

---

## 4. Jira vs. Development Flow — Match Check & Reconstruction

Every Jira task mapped to its pipeline stage and to the 17-step roadmap in `architecture.md §8`.

| Jira | Task | Stage | Roadmap step | Verdict |
|---|---|---|---|---|
| KAN-10 | rename dataset → `bronze_dev` | Bronze | 5 | ✅ matches · **Done** |
| KAN-11 | load 7 tables → `bronze_dev` | Bronze | 4 | ✅ matches · **Done** |
| KAN-12 | dockerize extractor + Cloud Run (dev) | Extract | — | ⚠️ scope — note A |
| KAN-13 | `silver_customers` typed cast | Silver | 6 | ✅ matches |
| KAN-14 | silver orders/items/payments/suppliers | Silver | 7 | ✅ matches |
| KAN-15 | SCD2 customers | Silver | 8 | ✅ matches |
| KAN-16 | SCD2 products | Silver | 8 | ✅ matches |
| KAN-17 | category hierarchy path | Silver | (doc §4.4) | ✅ matches |
| KAN-18 | dbt scaffold + profiles | Gold | 9 | ✅ matches |
| KAN-19 | dbt staging models | Gold | 9 | ✅ matches |
| KAN-20 | marts: fct_orders + fct_revenue_daily | Gold | 10 | ✅ matches |
| KAN-21 | marts: customer_360 + ar_aging + margin | Gold | 10 | ✅ matches |
| KAN-22 | dbt tests + docs | Gold | 11 | ✅ matches |
| KAN-23 | Metabase + 4 dashboards | Serve | 12 | ✅ matches |
| KAN-24 | Airflow DAG `retail_daily` | Orchestration | 13 | ✅ matches |
| KAN-25 | Terraform dev + prod | Foundation | 14 + 15 | ⚠️ bundles two — note B |
| KAN-26 | GitHub Actions CI/CD | Foundation | 16 | ✅ matches |
| KAN-27 | Cloud Monitoring alerts + log-based metrics | Orchestration | 17 | ✅ added — note C |

**Verdict: the board matches the flow well.** With KAN-27 added, 16 of 18 tasks line up
cleanly. Three findings came out of the check — one fixed, two left as known observations:

- **Note A — KAN-12 scope.** Titled *"dockerize **extractor**… (dev)"*, but the pipeline runs
  **two** Cloud Run Jobs — `extract_to_gcs.py` *and* `load_to_bq.py`. Recommend re-titling to
  *"Containerise extract + load jobs, deploy to Cloud Run (dev)"* so the load job isn't lost.
  Prod deployment is fine to fold into KAN-25.
- **Note B — KAN-25 bundles two roadmap steps** (14 = dev infra, 15 = promote to prod). Big
  but acceptable as one task; optionally split into `KAN-25 Terraform (dev)` and a new
  `KAN-? Terraform promote to prod`.
- **Note C — Monitoring (resolved ✅).** Roadmap step 17 ("Cloud Monitoring alerts on DAG
  failures") had **no Jira task**. Now created as **KAN-27** under Epic KAN-8 and assigned
  to you — the monitoring gap is closed.

> Source layer (schema, generator, extractor) has no Epic — it was built before Jira existed.
> Purely cosmetic; a closed "Phase 0" epic would make the board 100% complete, or leave as-is.

---

## 5. Where We Are Right Now

```
✅ Source (Postgres + 7 tables + generator)
✅ Extract script + Bronze load script        ← KAN-10, KAN-11 done
🟡 Containerise + Cloud Run deploy             ← KAN-12 IN PROGRESS  ← you are here
⬜ Silver (Dataproc/PySpark)                    ← KAN-13 next
⬜ Gold (dbt) → Serve (Metabase)
⬜ Orchestration (Composer) → Foundation (Terraform, GitHub Actions)
```

**Immediate next step:** finish KAN-12 — write the `Dockerfile`, push the image to Artifact
Registry, create the Cloud Run Job, and wire Cloud Scheduler to it.
