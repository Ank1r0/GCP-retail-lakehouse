"""
retail-lakehouse :: Postgres -> GCS bronze extractor  (incremental + idempotent)

Writes gzipped CSV — no pandas/pyarrow needed, loads natively into BigQuery.

How it works
------------
1. Load watermarks from gs://<bucket>/metadata/watermarks.json
2. For each table SELECT only rows newer than the watermark
3. Write  gs://<bucket>/bronze/retail__<table>/dt=<date>/run=<YYYYMMDD_HHMM>.csv.gz
4. Save updated watermarks back to GCS (only on full success)

Idempotency:  re-running at the same minute overwrites the same blob -> safe.
Scheduling:   Cloud Scheduler + Cloud Run Job (prod) or Task Scheduler (dev).

Auth: gcloud auth application-default login  (run once)

Usage:
    python extract_to_gcs.py --table orders          # single table
    python extract_to_gcs.py --all                   # all tables
    python extract_to_gcs.py --all --dry-run         # verify connections, no upload
    python extract_to_gcs.py --all --full            # ignore watermark, full extract
"""

import argparse
import csv
import gzip
import io
import json
import os
from datetime import datetime, timezone

import psycopg2
from dotenv import load_dotenv
from google.cloud import storage

from pathlib import Path
_DEFAULT_ENV = Path(__file__).parent.parent / "envs" / "dev.env"
load_dotenv(os.getenv("DOTENV_PATH") or _DEFAULT_ENV)

# ------------------------------------------------------------------
# Config
# ------------------------------------------------------------------
PG_HOST     = os.getenv("PG_HOST",     "localhost")
PG_PORT     = int(os.getenv("PG_PORT", "5432"))
PG_DATABASE = os.getenv("PG_DATABASE", "postgres")
PG_USER     = os.getenv("PG_USER",     "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "pagila")
GCS_PROJECT = os.getenv("GCS_PROJECT", "")
GCS_BUCKET  = os.getenv("GCS_BUCKET",  "retail-lakehouse-dev")
BRONZE      = "bronze"
WATERMARK_BLOB = "metadata/watermarks.json"

# incremental_col  — column used to detect new/changed rows
# col_type         — "timestamp" or "id" (integer PK)
# full_extract     — always pull all rows (small lookup tables)
TABLE_CONFIG = {
    "customers":   {"incremental_col": "updated_at",    "col_type": "timestamp"},
    "orders":      {"incremental_col": "updated_at",    "col_type": "timestamp"},
    "products":    {"incremental_col": "updated_at",    "col_type": "timestamp"},
    "order_items": {"incremental_col": "order_item_id", "col_type": "id"},
    "payments":    {"incremental_col": "payment_date",  "col_type": "timestamp"},
    "categories":  {"full_extract": True},
    "suppliers":   {"full_extract": True},
}

ALL_TABLES = list(TABLE_CONFIG.keys())


# ------------------------------------------------------------------
# Watermarks  (persisted in GCS as JSON)
# ------------------------------------------------------------------

def load_watermarks(client: storage.Client) -> dict:
    """Load the watermark file from GCS.

    The watermark file is a JSON object that remembers the highest
    updated_at / payment_date / order_item_id seen in the previous run
    for each table. On the next run we only fetch rows newer than that
    value, so we never re-extract rows we already have.

    If the file does not exist yet (first ever run), returns an empty
    dict — which causes read_table() to do a full extract of everything.

    Example watermarks.json content:
        {
          "customers": "2026-05-14 10:30:00",
          "orders":    "2026-05-14 10:30:00",
          "order_items": "9847"
        }
    """
    blob = client.bucket(GCS_BUCKET).blob(WATERMARK_BLOB)
    if not blob.exists():
        return {}
    return json.loads(blob.download_as_text())


def save_watermarks(client: storage.Client, watermarks: dict) -> None:
    """Persist updated watermarks back to GCS after a successful run.

    Only called at the very end of main() and only when at least one
    table was uploaded. This guarantees that a failed mid-run does not
    advance the watermark — the next run will safely re-extract the
    same rows (idempotent because the blob path includes the run
    timestamp, so re-uploading just overwrites the same file).
    """
    blob = client.bucket(GCS_BUCKET).blob(WATERMARK_BLOB)
    blob.upload_from_string(
        json.dumps(watermarks, indent=2, default=str),
        content_type="application/json",
    )
    print(f"  watermarks saved  ->  gs://{GCS_BUCKET}/{WATERMARK_BLOB}")


# ------------------------------------------------------------------
# Postgres
# ------------------------------------------------------------------

def pg_connect():
    """Open a connection to Postgres using credentials from the env file.

    Returns a psycopg2 connection object. The caller is responsible for
    closing it. All connection parameters come from environment variables
    loaded at the top of the module (PG_HOST, PG_PORT, etc.), which in
    turn come from envs/dev.env or envs/prod.env depending on DOTENV_PATH.
    """
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DATABASE,
        user=PG_USER, password=PG_PASSWORD,
    )


def read_table(conn, table: str, watermarks: dict, full: bool):
    """Query one table from Postgres and return only the rows we need.

    Three possible extraction modes, chosen automatically per table:

    1. Full extract (categories, suppliers, or --full flag):
       SELECT * FROM retail.<table>
       Used for small lookup tables that have no updated_at column and
       change rarely. Simpler to just reload them completely each time.

    2. First-ever incremental run (no watermark saved yet):
       SELECT * FROM retail.<table> ORDER BY <incremental_col>
       Fetches everything so we have a baseline, then saves the max
       value as the starting watermark for future runs.

    3. Incremental run (watermark exists):
       SELECT * FROM retail.<table> WHERE <col> > <last_value>
       Only fetches rows added or updated since the previous run.
       Works for both timestamp columns (updated_at, payment_date)
       and integer PK columns (order_item_id — no updated_at on that
       table, but items are never updated so the PK is a safe proxy).

    Returns:
        cols           — list of column names (used as CSV header)
        rows           — list of tuples (one per row)
        new_watermark  — the highest value seen in this batch,
                         or None for full-extract tables
    """
    cfg = TABLE_CONFIG[table]

    if cfg.get("full_extract") or full:
        query = f"SELECT * FROM retail.{table}"
        with conn.cursor() as cur:
            cur.execute(query)
            cols = [d[0] for d in cur.description]
            rows = cur.fetchall()
        return cols, rows, None

    col      = cfg["incremental_col"]
    col_type = cfg["col_type"]
    last     = watermarks.get(table)

    if last is None:
        query = f"SELECT * FROM retail.{table} ORDER BY {col}"
    elif col_type == "timestamp":
        query = f"SELECT * FROM retail.{table} WHERE {col} > %s ORDER BY {col}"
    else:
        query = f"SELECT * FROM retail.{table} WHERE {col} > %s ORDER BY {col}"

    with conn.cursor() as cur:
        cur.execute(query, (last,) if last is not None else ())
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()

    # New watermark = max value in this batch
    new_watermark = None
    if rows:
        col_idx       = cols.index(col)
        new_watermark = str(max(row[col_idx] for row in rows))

    return cols, rows, new_watermark


# ------------------------------------------------------------------
# GCS upload  (gzipped CSV, no compiled extensions needed)
# ------------------------------------------------------------------

def rows_to_csv_gz(cols: list, rows: list) -> bytes:
    """Serialise query results to a gzipped CSV byte string.

    The first row is the header (column names). All values are written
    using Python's csv module with QUOTE_MINIMAL — only quotes fields
    that contain the delimiter, a quote character, or a newline.

    Returns raw bytes ready to be uploaded directly to GCS without
    writing a temporary file to disk. Keeping it in memory is fine
    for tables up to a few hundred MB; for larger tables Parquet would
    be more efficient but requires pyarrow.
    """
    buf    = io.StringIO()
    writer = csv.writer(buf, quoting=csv.QUOTE_MINIMAL)
    writer.writerow(cols)
    writer.writerows(rows)
    return gzip.compress(buf.getvalue().encode("utf-8"))


def upload(cols: list, rows: list, table: str, run_ts: str, dry_run: bool) -> str:
    """Upload one table's rows as a gzipped CSV file to GCS.

    The destination path follows the Hive-partitioning convention so
    BigQuery can use it as a partition filter:
        bronze/retail__<table>/dt=YYYY-MM-DD/run=YYYYMMDD_HHMM.csv.gz

    - dt=   is the calendar date of this run (used by BQ as a DATE partition).
    - run=  is the full timestamp, making the blob name unique per minute.
             Re-running at the same minute overwrites the same blob,
             so the operation is idempotent.

    In dry-run mode the path is printed but nothing is uploaded,
    which is useful for verifying GCP connectivity and table config
    without touching any data.

    Returns the full gs:// URI of the uploaded (or would-be) file.
    """
    dt        = run_ts[:8]
    date_str  = f"{dt[:4]}-{dt[4:6]}-{dt[6:]}"
    blob_path = f"{BRONZE}/retail__{table}/dt={date_str}/run={run_ts}.csv.gz"
    gcs_uri   = f"gs://{GCS_BUCKET}/{blob_path}"

    if dry_run:
        print(f"    [dry-run] {len(rows):,} rows  ->  {gcs_uri}")
        return gcs_uri

    data = rows_to_csv_gz(cols, rows)

    client = storage.Client(project=GCS_PROJECT or None)
    blob   = client.bucket(GCS_BUCKET).blob(blob_path)
    blob.upload_from_string(data, content_type="application/gzip")

    print(f"    {len(rows):,} rows  {len(data)/1024:.1f} KB  ->  {gcs_uri}")
    return gcs_uri


# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------

def main():
    """Entry point — parse CLI args and run the extraction pipeline.

    Orchestrates the full flow:
      1. Parse --table / --all / --dry-run / --full flags.
      2. Generate a run timestamp (YYYYMMDD_HHMM) shared across all tables
         so every file from this run sorts together in GCS.
      3. Load watermarks from GCS so we know where each table left off.
      4. Connect to Postgres.
      5. For each target table: read new rows, upload to GCS, record new watermark.
      6. Save updated watermarks to GCS (only if something was uploaded).

    Watermarks are saved only after all tables succeed. If the script
    crashes mid-run, the watermark stays at the previous value and the
    next run re-extracts the same rows — safe because blob names are
    deterministic (same run_ts = same blob path = overwrite).
    """
    parser = argparse.ArgumentParser(description="Incremental Postgres -> GCS bronze extractor")
    group  = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--table",   choices=ALL_TABLES)
    group.add_argument("--all",     action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--full",    action="store_true", help="Ignore watermark")
    args = parser.parse_args()

    tables = ALL_TABLES if args.all else [args.table]
    run_ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")

    print(f"Run: {run_ts}  |  target: gs://{GCS_BUCKET}/{BRONZE}/")
    if args.dry_run:
        print("DRY RUN — nothing will be uploaded\n")

    gcs_client = storage.Client(project=GCS_PROJECT or None)
    watermarks = load_watermarks(gcs_client)

    if watermarks:
        print(f"Watermarks loaded for: {', '.join(watermarks.keys())}")
    else:
        print("No watermarks — first run, extracting everything")

    print(f"\nConnecting to Postgres {PG_HOST}:{PG_PORT}/{PG_DATABASE}")
    conn = pg_connect()
    print("Connected.\n")

    new_watermarks = dict(watermarks)
    uploaded_any   = False

    for table in tables:
        print(f"[{table}]")
        cols, rows, new_wm = read_table(conn, table, watermarks, full=args.full)

        if not rows:
            print(f"    no new rows since last run — skipping")
            continue

        upload(cols, rows, table, run_ts, args.dry_run)
        uploaded_any = True

        if new_wm is not None and not args.dry_run:
            new_watermarks[table] = new_wm
            print(f"    watermark  ->  {new_wm}")

    conn.close()

    if not args.dry_run and new_watermarks != watermarks:
        print()
        save_watermarks(gcs_client, new_watermarks)

    print("\nDone.")


if __name__ == "__main__":
    main()
