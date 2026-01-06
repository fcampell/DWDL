import os
import json
import urllib.request
import boto3
import hashlib
from datetime import datetime, timezone

from common_ingestion import (
    utc_now_iso,
    log_event,
    audit_writer,
    s3_state_reader_writer,
    standard_audit_payload,
)

# ---------- AWS clients ----------
s3_client = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")

# ---------- Config ----------
BUCKET_NAME = "facade-project-dev"
BASE_URL = "https://data.stadt-zuerich.ch/dataset/vbz_fahrgastzahlen_ogd/download"

# Layout (aligned to your other datasets)
STATE_KEY = "bronze/public_transport_vbz/state.json"

BRONZE_BASE = "bronze/public_transport_vbz"
SILVER_BASE = "silver/public_transport_vbz"

# Audit
AUDIT_TABLE = os.environ.get("AUDIT_TABLE", "ingestion_audit")
DATASET_NAME = os.environ.get("DATASET_NAME", "public_transport_vbz")

write_audit = audit_writer(dynamodb, DATASET_NAME, AUDIT_TABLE)
read_state, write_state = s3_state_reader_writer(s3_client, BUCKET_NAME, STATE_KEY)


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def download_bytes(url: str, timeout: int = 120) -> bytes:
    with urllib.request.urlopen(url, timeout=timeout) as response:
        return response.read()


def put_bytes(bucket: str, key: str, body: bytes, content_type: str) -> None:
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType=content_type,
    )


def lambda_handler(event, context):
    ingested_at = utc_now_iso()

    # We keep a "year" folder to match your target structure.
    # Note: VBZ REISENDE is published as a single file (no year field in schema),
    # so this year is a storage/organization choice.
    year = datetime.now(timezone.utc).year
    if isinstance(event, dict) and isinstance(event.get("year"), int):
        year = event["year"]
    year_str = str(year)

    bronze_year_prefix = f"s3://{BUCKET_NAME}/{BRONZE_BASE}/{year}/"
    silver_year_prefix = f"s3://{BUCKET_NAME}/{SILVER_BASE}/{year}/"
    bronze_ref_prefix = f"s3://{BUCKET_NAME}/{BRONZE_BASE}/reference/"
    silver_ref_prefix = f"s3://{BUCKET_NAME}/{SILVER_BASE}/reference/"

    # Units to fetch:
    # - FACT:   REISENDE.(csv/parquet)
    # - REFS:   HALTESTELLEN / LINIE / TAGTYP / GEFAESSGROESSE as (csv/parquet)
    units = {
        "REISENDE": {
            "csv_url": f"{BASE_URL}/REISENDE.csv",
            "parquet_url": f"{BASE_URL}/REISENDE.parquet",
            "bronze_key": f"{BRONZE_BASE}/{year}/zurich_public_transport_vbz_{year}.csv",
            "silver_key": f"{SILVER_BASE}/{year}/public_transport_vbz_{year}.parquet",
            "bronze_content_type": "text/csv",
            "silver_content_type": "application/octet-stream",
            "state_path": ["fact", year_str, "REISENDE"],
            "audit_unit": "fact",
        },
        "HALTESTELLEN": {
            "csv_url": f"{BASE_URL}/HALTESTELLEN.csv",
            "parquet_url": f"{BASE_URL}/HALTESTELLEN.parquet",
            "bronze_key": f"{BRONZE_BASE}/reference/haltestellen.csv",
            "silver_key": f"{SILVER_BASE}/reference/haltestellen.parquet",
            "bronze_content_type": "text/csv",
            "silver_content_type": "application/octet-stream",
            "state_path": ["reference", "HALTESTELLEN"],
            "audit_unit": "reference",
        },
        "LINIE": {
            "csv_url": f"{BASE_URL}/LINIE.csv",
            "parquet_url": f"{BASE_URL}/LINIE.parquet",
            "bronze_key": f"{BRONZE_BASE}/reference/linie.csv",
            "silver_key": f"{SILVER_BASE}/reference/linie.parquet",
            "bronze_content_type": "text/csv",
            "silver_content_type": "application/octet-stream",
            "state_path": ["reference", "LINIE"],
            "audit_unit": "reference",
        },
        "TAGTYP": {
            "csv_url": f"{BASE_URL}/TAGTYP.csv",
            "parquet_url": f"{BASE_URL}/TAGTYP.parquet",
            "bronze_key": f"{BRONZE_BASE}/reference/tagtyp.csv",
            "silver_key": f"{SILVER_BASE}/reference/tagtyp.parquet",
            "bronze_content_type": "text/csv",
            "silver_content_type": "application/octet-stream",
            "state_path": ["reference", "TAGTYP"],
            "audit_unit": "reference",
        },
        "GEFAESSGROESSE": {
            "csv_url": f"{BASE_URL}/GEFAESSGROESSE.csv",
            "parquet_url": f"{BASE_URL}/GEFAESSGROESSE.parquet",
            "bronze_key": f"{BRONZE_BASE}/reference/gefaessgroesse.csv",
            "silver_key": f"{SILVER_BASE}/reference/gefaessgroesse.parquet",
            "bronze_content_type": "text/csv",
            "silver_content_type": "application/octet-stream",
            "state_path": ["reference", "GEFAESSGROESSE"],
            "audit_unit": "reference",
        },
    }

    results = {}
    totals = {
        "units_total": len(units),
        "units_success": 0,
        "units_no_new_data": 0,
        "units_failed": 0,
    }

    def state_get_path(state_obj: dict, path_parts: list[str]) -> dict:
        cur = state_obj
        for p in path_parts:
            nxt = cur.get(p)
            if not isinstance(nxt, dict):
                return {}
            cur = nxt
        return cur

    def state_set_path(state_obj: dict, path_parts: list[str], value: dict) -> None:
        cur = state_obj
        for p in path_parts[:-1]:
            if p not in cur or not isinstance(cur[p], dict):
                cur[p] = {}
            cur = cur[p]
        cur[path_parts[-1]] = value

    try:
        log_event(
            "info",
            {
                "event": "ingestion_start",
                "dataset": DATASET_NAME,
                "ingested_at": ingested_at,
                "year": year,
                "lambda_request_id": getattr(context, "aws_request_id", None),
                "function_name": getattr(context, "function_name", None),
                "bronze_year_prefix": bronze_year_prefix,
                "silver_year_prefix": silver_year_prefix,
                "bronze_ref_prefix": bronze_ref_prefix,
                "silver_ref_prefix": silver_ref_prefix,
            },
        )

        state = read_state()

        for name, cfg in units.items():
            unit_bronze = f"s3://{BUCKET_NAME}/{cfg['bronze_key']}"
            unit_silver = f"s3://{BUCKET_NAME}/{cfg['silver_key']}"

            # Load previous watermark (sha256) for this unit
            prev_state = state_get_path(state, cfg["state_path"])
            prev_hash = prev_state.get("sha256")

            try:
                log_event(
                    "info",
                    {
                        "event": "unit_download_start",
                        "dataset": DATASET_NAME,
                        "unit": name,
                        "csv_url": cfg["csv_url"],
                        "parquet_url": cfg["parquet_url"],
                    },
                )

                # Download CSV + Parquet from the official URLs
                csv_bytes = download_bytes(cfg["csv_url"], timeout=120)
                pq_bytes = download_bytes(cfg["parquet_url"], timeout=120)

                # Use CSV hash as watermark (stable for "has anything changed?")
                new_hash = sha256_bytes(csv_bytes)

                if prev_hash and new_hash == prev_hash:
                    totals["units_no_new_data"] += 1
                    results[name] = {
                        "status": "no_new_data",
                        "watermark": new_hash,
                        "bronze": unit_bronze,
                        "silver": unit_silver,
                    }

                    log_event(
                        "info",
                        {
                            "event": "unit_no_new_data",
                            "dataset": DATASET_NAME,
                            "unit": name,
                            "reason": "sha256_unchanged",
                            "sha256": new_hash,
                            "bronze": unit_bronze,
                            "silver": unit_silver,
                        },
                    )

                    payload = standard_audit_payload(context, f"{DATASET_NAME}#{name}", ingested_at)
                    payload.update(
                        {
                            "status": "no_new_data",
                            "records": 0,
                            "year": year,
                            "watermark": new_hash,
                            "s3_location": unit_bronze,
                            "message": "No new file content (sha256 unchanged)",
                        }
                    )
                    write_audit(payload)
                    continue

                # Write Bronze CSV (overwrite)
                put_bytes(
                    BUCKET_NAME,
                    cfg["bronze_key"],
                    csv_bytes,
                    cfg["bronze_content_type"],
                )

                # Write Silver Parquet (overwrite)
                put_bytes(
                    BUCKET_NAME,
                    cfg["silver_key"],
                    pq_bytes,
                    cfg["silver_content_type"],
                )

                # Update state for this unit
                state_set_path(
                    state,
                    cfg["state_path"],
                    {
                        "sha256": new_hash,
                        "updated_at_utc": ingested_at,
                        "bronze_key": cfg["bronze_key"],
                        "silver_key": cfg["silver_key"],
                    },
                )

                totals["units_success"] += 1
                results[name] = {
                    "status": "success",
                    "watermark": new_hash,
                    "bronze": unit_bronze,
                    "silver": unit_silver,
                }

                log_event(
                    "info",
                    {
                        "event": "unit_success",
                        "dataset": DATASET_NAME,
                        "unit": name,
                        "sha256": new_hash,
                        "bronze": unit_bronze,
                        "silver": unit_silver,
                        "bytes_csv": len(csv_bytes),
                        "bytes_parquet": len(pq_bytes),
                    },
                )

                payload = standard_audit_payload(context, f"{DATASET_NAME}#{name}", ingested_at)
                payload.update(
                    {
                        "status": "success",
                        "records": 0,
                        "year": year,
                        "watermark": new_hash,
                        "s3_location": unit_bronze,
                        "message": json.dumps({"bronze": unit_bronze, "silver": unit_silver}),
                    }
                )
                write_audit(payload)

            except Exception as e:
                totals["units_failed"] += 1
                results[name] = {"status": "error", "error": str(e)}

                log_event(
                    "error",
                    {
                        "event": "unit_error",
                        "dataset": DATASET_NAME,
                        "unit": name,
                        "error": str(e),
                        "bronze": unit_bronze,
                        "silver": unit_silver,
                    },
                )

                payload = standard_audit_payload(context, f"{DATASET_NAME}#{name}", ingested_at)
                payload.update(
                    {
                        "status": "error",
                        "records": 0,
                        "year": year,
                        "s3_location": unit_bronze,
                        "error": str(e),
                    }
                )
                write_audit(payload)

        # Persist state once at end
        state["updated_at_utc"] = ingested_at
        write_state(state)

        run_status = "success" if totals["units_failed"] == 0 else "partial"
        status_code = 200 if run_status == "success" else 207

        log_event(
            "info",
            {
                "event": "ingestion_complete",
                "dataset": DATASET_NAME,
                "ingested_at": ingested_at,
                "status": run_status,
                "year": year,
                "totals": totals,
                "bronze_base": f"s3://{BUCKET_NAME}/{BRONZE_BASE}/",
                "silver_base": f"s3://{BUCKET_NAME}/{SILVER_BASE}/",
                "state_key": STATE_KEY,
            },
        )

        # Run-level audit record
        payload = standard_audit_payload(context, DATASET_NAME, ingested_at)
        payload.update(
            {
                "status": run_status,
                "records": 0,
                "year": year,
                "watermark": "",
                "s3_location": f"s3://{BUCKET_NAME}/{BRONZE_BASE}/",
                "message": json.dumps({"totals": totals, "results": results}),
            }
        )
        write_audit(payload)

        return {
            "statusCode": status_code,
            "body": json.dumps(
                {
                    "message": f"{totals['units_success']} updated, {totals['units_no_new_data']} unchanged, {totals['units_failed']} failed",
                    "status": run_status,
                    "timestamp": ingested_at,
                    "totals": totals,
                    "results": results,
                    "bronze_base": f"s3://{BUCKET_NAME}/{BRONZE_BASE}/",
                    "silver_base": f"s3://{BUCKET_NAME}/{SILVER_BASE}/",
                }
            ),
        }

    except Exception as e:
        log_event(
            "error",
            {
                "event": "ingestion_fatal_error",
                "dataset": DATASET_NAME,
                "ingested_at": ingested_at,
                "year": year,
                "error": str(e),
                "state_key": STATE_KEY,
            },
        )

        payload = standard_audit_payload(context, DATASET_NAME, ingested_at)
        payload.update(
            {
                "status": "error",
                "records": 0,
                "year": year,
                "s3_location": f"s3://{BUCKET_NAME}/{BRONZE_BASE}/",
                "error": str(e),
            }
        )
        write_audit(payload)

        return {"statusCode": 500, "body": json.dumps({"message": f"Error: {str(e)}"})}
