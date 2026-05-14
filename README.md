# retail-lakehouse

End-to-end analytics project: **Postgres (Docker) → GCS → BigQuery → dbt → Metabase**, running on GCP.

This repo currently contains the **source OLTP layer** — the schema and a synthetic data generator that simulates an on-prem operational database. Subsequent layers (extractor → GCS → BigQuery → dbt → Metabase) will plug into this.

## Architecture (target)

```
┌────────────────┐    ┌─────────────┐    ┌────────────┐    ┌──────────┐    ┌──────────┐
│  Postgres      │───▶│  Python     │───▶│  GCS       │───▶│ BigQuery │───▶│ Metabase │
│  (Docker)      │    │  extractor  │    │  bronze/   │    │ raw →    │    │          │
│  retail.*      │    │  Parquet    │    │            │    │ staging →│    │          │
│                │    │             │    │            │    │ marts    │    │          │
└────────────────┘    └─────────────┘    └────────────┘    └──────────┘    └──────────┘
                                                                ▲
                                                                │
                                                         ┌─────────────┐
                                                         │  dbt-bigquery│
                                                         │  staging +   │
                                                         │  marts + tests│
                                                         └─────────────┘
```

## Schema (`retail` schema)

| Table | Type | Notes |
|---|---|---|
| `customers` | Dim | `status` ∈ active/inactive/churned, signup over last 3y |
| `categories` | Dim | Self-referencing hierarchy (parent_category_id) |
| `suppliers` | Dim | Vendor master |
| `products` | Dim | Linked to category + supplier, has cost & price → margin |
| `orders` | Fact (header) | Status enum, multi-status lifecycle |
| `order_items` | Fact (line) | Quantity, discount, line_amount |
| `payments` | Fact | Multiple methods, some delayed (AR-aging) |

**What downstream layers can do with this:**
- **Spark**: BKPF/BSEG-style joins on orders + order_items at scale, broadcast joins on small dims.
- **dbt**: staging (1:1 cleansed), marts (revenue, customer 360, AR aging, product margin), tests on PK / FK / business rules.
- **SCD2**: customer status changes, product price changes.
- **Recursive CTE**: walk category hierarchy (parent → children → grandchildren).
- **AR aging**: orders left without a captured payment within N days.

## Quick start

Both containers are already running (`postgres-training` on `:5432`, `metabase` on `:3000`).

### 1. Create the schema

```powershell
docker exec -i postgres-training psql -U postgres -d postgres < db\01_schema.sql
```

### 2. Install generator dependencies

```powershell
cd generator
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
copy .env.example .env
```

Edit `.env` if your Postgres credentials differ.

### 3. Generate data

```powershell
python generate.py --truncate
```

Defaults: 1k customers, 200 products, 50 suppliers, 10k orders (~25k items, ~9.5k payments).

Scale up:
```powershell
python generate.py --truncate --customers 10000 --orders 200000
```

### 4. Verify in Metabase

Open <http://localhost:3000> → Settings → Admin → Databases → Add database → PostgreSQL:

| Field | Value |
|---|---|
| Display name | `retail (local pg)` |
| Host | `host.docker.internal` |
| Port | `5432` |
| Database name | `postgres` |
| Username | `postgres` |
| Password | `pagila` |
| Schemas | `retail` (or leave All) |

After sync, all 7 tables should appear under the `retail` schema.

### Sample queries to validate

```sql
-- Order status distribution
SELECT status, COUNT(*) FROM retail.orders GROUP BY status ORDER BY 2 DESC;

-- AR aging candidates (orders with no captured payment)
SELECT COUNT(*) FROM retail.orders o
WHERE NOT EXISTS (
  SELECT 1 FROM retail.payments p
  WHERE p.order_id = o.order_id AND p.status = 'captured'
)
AND o.status <> 'cancelled';

-- Recursive walk of the category tree
WITH RECURSIVE tree AS (
  SELECT category_id, category_name, parent_category_id, 1 AS lvl
  FROM retail.categories WHERE parent_category_id IS NULL
  UNION ALL
  SELECT c.category_id, c.category_name, c.parent_category_id, t.lvl + 1
  FROM retail.categories c JOIN tree t ON c.parent_category_id = t.category_id
)
SELECT * FROM tree ORDER BY lvl, category_name;
```

## Next steps (not yet built)

1. **`extract/`** — Python script: Postgres → Parquet → `gs://<bucket>/bronze/<table>/dt=YYYY-MM-DD/`. Snapshot + incremental modes.
2. **`bq/`** — BigQuery dataset DDL + external tables / native loads from GCS.
3. **`dbt/`** — `dbt-bigquery` project with `staging` (1:1 cleansed) and `marts` (revenue, customer_360, ar_aging, product_margin) layers + tests.
4. **`spark/`** — PySpark notebook(s) for the heavier joins (order_items × products × categories with recursive hierarchy).
5. **`orchestration/`** — Cloud Composer DAG or local Prefect flow to chain the above.
6. **Metabase** — re-point at BigQuery once marts exist; build executive dashboards on top.

## Layout

```
retail-lakehouse/
├── README.md
├── db/
│   └── 01_schema.sql          # OLTP schema (run against postgres-training)
└── generator/
    ├── generate.py            # Faker-based synthetic data
    ├── requirements.txt
    └── .env.example
```
