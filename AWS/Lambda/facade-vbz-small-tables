import os
import json
import urllib.request
import boto3
import csv
import hashlib
import time
from io import StringIO
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
STATE_KEY = "bronze/vbz/state_reference_tables.json"

AUDIT_TABLE = os.environ.get("AUDIT_TABLE", "ingestion_audit")
DATASET_NAME = os.environ.get("DATASET_NAME", "vbz_reference_tables")

TABLES = {
    "HALTESTELLEN": f"{BASE_URL}/HALTESTELLEN.csv",
    "LINIE": f"{BASE_URL}/LINIE.csv",
    "TAGTYP": f"{BASE_URL}/TAGTYP.csv",
    "GEFAESSGROESSE": f"{BASE_URL}/GEFAESSGROESSE.csv",
}

write_audit = audit_writer(dynamodb, DATASET_NAME, AUDIT_TABLE)
read_state, write_state = s3_state_reader_writer(s3_client, BUCKET_NAME, STATE_KEY)


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def parse_count_rows(csv_text: str, delimiter: str = ";") -> int:
    reader = csv.DictReader(StringIO(csv_text), delimiter=delimiter)
    return sum(1 for _ in reader)


def table_audit_payload(context, ingested_at: str, table_name: str) -> dict:
    """
    For table-level audit rows we keep your previous pattern:
    dataset = "<DATASET_NAME>#<TABLE>"
    """
    base = standard_audit_payload(context, f"{DATASET_NAME}#{table_name}", ingested_at)
    return base


def lambda_handler(event, context):
    ingested_at = utc_now_iso()
    year = datetime.now(timezone.utc).year
    run_start = time.time()

    results = {}
    totals = {
        "tables_total": len(TABLES),
        "tables_success": 0,
        "tables_no_new_data": 0,
        "tables_failed": 0,
        "records_written": 0,
    }

    log_event(
        "info",
        {
            "event": "ingestion_start",
            "dataset": DATASET_NAME,
            "ingested_at": ingested_at,
            "year": year,
            "tables": list(TABLES.keys()),
            "lambda_request_id": getattr(context, "aws_request_id", None),
            "function_name": getattr(context, "function_name", None),
        },
    )

    state = read_state()
    state_tables = state.get("tables", {}) if isinstance(state.get("tables", {}), dict) else {}

    try:
        for table_name, url in TABLES.items():
            t0 = time.time()
            s3_key = f"bronze/vbz/{table_name.lower()}.csv"
            s3_location = f"s3://{BUCKET_NAME}/{s3_key}"
            prev_hash = (state_tables.get(table_name) or {}).get("sha256")

            try:
                log_event(
                    "info",
                    {
                        "event": "table_download_start",
                        "dataset": DATASET_NAME,
                        "table": table_name,
                        "url": url,
                        "ingested_at": ingested_at,
                    },
                )

                with urllib.request.urlopen(url, timeout=120) as response:
                    csv_text = response.read().decode("utf-8")

                new_hash = sha256_text(csv_text)

                if prev_hash and new_hash == prev_hash:
                    duration_ms = int((time.time() - t0) * 1000)

                    results[table_name] = {
                        "status": "no_new_data",
                        "records": 0,
                        "s3_location": s3_location,
                        "sha256": new_hash,
                        "duration_ms": duration_ms,
                    }
                    totals["tables_no_new_data"] += 1

                    log_event(
                        "info",
                        {
                            "event": "table_no_new_data",
                            "dataset": DATASET_NAME,
                            "table": table_name,
                            "sha256": new_hash,
                            "s3_location": s3_location,
                            "duration_ms": duration_ms,
                        },
                    )

                    payload = table_audit_payload(context, ingested_at, table_name)
                    payload.update(
                        {
                            "status": "no_new_data",
                            "records": 0,
                            "year": year,
                            "watermark": new_hash,
                            "s3_location": s3_location,
                            "message": "No new file content (sha256 unchanged)",
                        }
                    )
                    write_audit(payload)
                    continue

                record_count = parse_count_rows(csv_text, delimiter=";")

                s3_client.put_object(
                    Bucket=BUCKET_NAME,
                    Key=s3_key,
                    Body=csv_text.encode("utf-8"),
                    ContentType="text/csv",
                )

                state_tables[table_name] = {
                    "sha256": new_hash,
                    "updated_at_utc": ingested_at,
                    "records": int(record_count),
                    "s3_key": s3_key,
                }

                duration_ms = int((time.time() - t0) * 1000)

                results[table_name] = {
                    "status": "success",
                    "records": record_count,
                    "s3_location": s3_location,
                    "sha256": new_hash,
                    "duration_ms": duration_ms,
                }
                totals["tables_success"] += 1
                totals["records_written"] += int(record_count)

                log_event(
                    "info",
                    {
                        "event": "table_success",
                        "dataset": DATASET_NAME,
                        "table": table_name,
                        "records": record_count,
                        "sha256": new_hash,
                        "s3_location": s3_location,
                        "duration_ms": duration_ms,
                    },
                )

                payload = table_audit_payload(context, ingested_at, table_name)
                payload.update(
                    {
                        "status": "success",
                        "records": int(record_count),
                        "year": year,
                        "watermark": new_hash,
                        "s3_location": s3_location,
                        "message": "Downloaded and uploaded reference table CSV",
                    }
                )
                write_audit(payload)

            except Exception as e:
                duration_ms = int((time.time() - t0) * 1000)
                totals["tables_failed"] += 1

                results[table_name] = {
                    "status": "failed",
                    "error": str(e),
                    "s3_location": s3_location,
                    "duration_ms": duration_ms,
                }

                log_event(
                    "error",
                    {
                        "event": "table_error",
                        "dataset": DATASET_NAME,
                        "table": table_name,
                        "error": str(e),
                        "s3_location": s3_location,
                        "duration_ms": duration_ms,
                    },
                )

                payload = table_audit_payload(context, ingested_at, table_name)
                payload.update(
                    {
                        "status": "error",
                        "records": 0,
                        "year": year,
                        "s3_location": s3_location,
                        "error": str(e),
                    }
                )
                write_audit(payload)

        state["tables"] = state_tables
        state["updated_at_utc"] = ingested_at
        write_state(state)

        run_duration_ms = int((time.time() - run_start) * 1000)
        run_status = "success" if totals["tables_failed"] == 0 else "partial"

        log_event(
            "info",
            {
                "event": "ingestion_complete",
                "dataset": DATASET_NAME,
                "ingested_at": ingested_at,
                "status": run_status,
                "year": year,
                "totals": totals,
                "duration_ms": run_duration_ms,
                "s3_base_location": f"s3://{BUCKET_NAME}/bronze/vbz/",
            },
        )

        # Run-level audit summary row (kept)
        payload = standard_audit_payload(context, DATASET_NAME, ingested_at)
        payload.update(
            {
                "status": run_status,
                "records": int(totals["records_written"]),
                "year": year,
                "watermark": "",
                "s3_location": f"s3://{BUCKET_NAME}/bronze/vbz/",
                "message": json.dumps(totals),
            }
        )
        write_audit(payload)

        return {
            "statusCode": 200 if run_status != "partial" else 207,
            "body": json.dumps(
                {
                    "message": f"{totals['tables_success']}/{totals['tables_total']} tables updated, "
                               f"{totals['tables_no_new_data']} unchanged, "
                               f"{totals['tables_failed']} failed",
                    "timestamp": ingested_at,
                    "year": year,
                    "status": run_status,
                    "totals": totals,
                    "results": results,
                    "s3_base_location": f"s3://{BUCKET_NAME}/bronze/vbz/",
                },
                ensure_ascii=False,
            ),
        }

    except Exception as e:
        run_duration_ms = int((time.time() - run_start) * 1000)

        log_event(
            "error",
            {
                "event": "ingestion_fatal_error",
                "dataset": DATASET_NAME,
                "ingested_at": ingested_at,
                "year": year,
                "error": str(e),
                "duration_ms": run_duration_ms,
            },
        )

        payload = standard_audit_payload(context, DATASET_NAME, ingested_at)
        payload.update(
            {
                "status": "error",
                "records": 0,
                "year": year,
                "s3_location": f"s3://{BUCKET_NAME}/bronze/vbz/",
                "error": str(e),
            }
        )
        write_audit(payload)

        return {
            "statusCode": 500,
            "body": json.dumps({"error": f"Failed: {str(e)}", "timestamp": ingested_at}, ensure_ascii=False),
        }
