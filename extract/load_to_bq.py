"""
retail-lakehouse :: GCS bronze -> BigQuery bronze loader

Reads every CSV.GZ file under gs://<bucket>/bronze/retail__<table>/
and loads it into BigQuery  retail-496114.bronze.bronze_<table>.

BigQuery does all the work — the CSV never flows through Python.
Schema is autodetected from the CSV headers on first load; after that
it is updated to absorb any new columns (useful when the source adds fields).

Write strategy: WRITE_TRUNCATE
  Each run replaces the BQ table with a fresh load of all GCS files.
  GCS is the archive; BQ bronze is the current queryable snapshot.
  Silver (dbt) handles dedup, SCD2, and business logic.

Usage:
    python load_to_bq.py --all                    # load all tables
    python load_to_bq.py --table orders           # single table
    python load_to_bq.py --all --dry-run          # preview only, no BQ writes
"""

import argparse
import os
from datetime import datetime

from dotenv import load_dotenv
from google.cloud import bigquery, storage

from pathlib import Path
_DEFAULT_ENV = Path(__file__).parent.parent / "envs" / "dev.env"
load_dotenv(os.getenv("DOTENV_PATH") or _DEFAULT_ENV)

GCS_PROJECT = os.getenv("GCS_PROJECT", "")
GCS_BUCKET  = os.getenv("GCS_BUCKET",  "retail-lakehouse-dev")
BQ_DATASET  = os.getenv("BQ_DATASET_BRONZE", os.getenv("BQ_DATASET", "bronze_dev"))
BRONZE_PREFIX = "bronze"

TABLES = [
    "customers",
    "categories",
    "suppliers",
    "products",
    "orders",
    "order_items",
    "payments",
]


def list_gcs_files(gcs_client: storage.Client, table: str) -> list[str]:
    """Return all GCS URIs for a given table prefix."""
    prefix = f"{BRONZE_PREFIX}/retail__{table}/"
    blobs  = list(gcs_client.bucket(GCS_BUCKET).list_blobs(prefix=prefix))
    uris   = [f"gs://{GCS_BUCKET}/{b.name}" for b in blobs if b.name.endswith(".csv.gz")]
    return uris


def load_table(bq_client: bigquery.Client, table: str, uris: list[str], dry_run: bool):
    destination = f"{GCS_PROJECT}.{BQ_DATASET}.bronze_{table}"

    if dry_run:
        print(f"  [dry-run] would load {len(uris)} file(s) -> {destination}")
        for uri in uris[:3]:
            print(f"    {uri}")
        if len(uris) > 3:
            print(f"    ... and {len(uris) - 3} more")
        return

    job_config = bigquery.LoadJobConfig(
        source_format         = bigquery.SourceFormat.CSV,
        skip_leading_rows     = 1,           # CSV has header row
        autodetect            = True,        # infer column types
        write_disposition     = bigquery.WriteDisposition.WRITE_TRUNCATE,
        allow_quoted_newlines = True,
        allow_jagged_rows     = False,
    )

    print(f"  loading {len(uris)} file(s) -> {destination}")
    load_job = bq_client.load_table_from_uri(
        uris,
        destination,
        job_config=job_config,
    )

    load_job.result()   # blocks until BigQuery finishes

    table_ref  = bq_client.get_table(destination)
    print(f"  done — {table_ref.num_rows:,} rows, {len(table_ref.schema)} columns")


def main():
    parser = argparse.ArgumentParser(description="Load GCS bronze files into BigQuery")
    group  = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--table", choices=TABLES)
    group.add_argument("--all",   action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    tables = TABLES if args.all else [args.table]

    print(f"Project : {GCS_PROJECT}")
    print(f"Bucket  : gs://{GCS_BUCKET}/{BRONZE_PREFIX}/")
    print(f"Dataset : {GCS_PROJECT}.{BQ_DATASET}")
    if args.dry_run:
        print("DRY RUN — no BigQuery writes\n")
    else:
        print()

    gcs_client = storage.Client(project=GCS_PROJECT or None)
    bq_client  = bigquery.Client(project=GCS_PROJECT or None)

    for table in tables:
        print(f"[{table}]")
        uris = list_gcs_files(gcs_client, table)

        if not uris:
            print(f"  no files found at gs://{GCS_BUCKET}/{BRONZE_PREFIX}/retail__{table}/")
            continue

        load_table(bq_client, table, uris, args.dry_run)

    print("\nDone.")
    if not args.dry_run:
        print(f"\nQuery your tables:")
        for t in tables:
            print(f"  SELECT * FROM `{GCS_PROJECT}.{BQ_DATASET}.bronze_{t}` LIMIT 10")


if __name__ == "__main__":
    main()
