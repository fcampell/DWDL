import os
import json
import urllib.request
import urllib.parse
import boto3
import csv
import io
import re
from datetime import datetime, timezone

import pandas as pd
import awswrangler as wr
from botocore.exceptions import ClientError

from ingestion_common import (
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
API_ENDPOINT_SQL = "https://data.stadt-zuerich.ch/api/3/action/datastore_search_sql"
RESOURCE_ID = "89b2179c-19a5-41dc-baac-3ee3d6a42927"
BUCKET_NAME = "facade-project-dev"

STATE_KEY = "bronze/motorized_traffic/state.json"
BRONZE_PREFIX = "bronze/motorized_traffic"
SILVER_PREFIX = "silver/motorized_traffic"

AUDIT_TABLE = os.environ.get("AUDIT_TABLE", "ingestion_audit")
DATASET_NAME = os.environ.get("DATASET_NAME", "motorized_traffic")

SQL_PAGE_SIZE = int(os.environ.get("SQL_PAGE_SIZE", "20000"))
MAX_PAGES = int(os.environ.get("MAX_PAGES", "1000000"))
MAX_SECONDS = int(os.environ.get("MAX_SECONDS", "1000000"))

# If true and _id exists, dedupe by _id; else dedupe by full row
DEDUP_ON_ID = os.environ.get("DEDUP_ON_ID", "true").lower() == "true"

write_audit = audit_writer(dynamodb, DATASET_NAME, AUDIT_TABLE)
read_state, write_state = s3_state_reader_writer(s3_client, BUCKET_NAME, STATE_KEY)


def s3_get_text_if_exists(bucket: str, key: str) -> str | None:
    try:
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        return obj["Body"].read().decode("utf-8")
    except ClientError as e:
        code = (e.response.get("Error", {}) or {}).get("Code")
        if code in ("NoSuchKey", "404"):
            return None
        raise


def s3_put_text(bucket: str, key: str, text: str, content_type: str) -> None:
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=text.encode("utf-8"),
        ContentType=content_type,
    )


def to_snake_case(name: str) -> str:
    name = name.strip()
    name = re.sub(r"[^\w]+", "_", name, flags=re.UNICODE)
    name = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)
    name = name.lower()
    name = re.sub(r"_+", "_", name).strip("_")
    if not name:
        return "col"
    if name[0].isdigit():
        name = f"col_{name}"
    return name


def normalize_columns_snake_case(df: pd.DataFrame) -> pd.DataFrame:
    mapping = {}
    used = set()
    for c in df.columns:
        base = to_snake_case(str(c))
        new = base
        i = 2
        while new in used:
            new = f"{base}_{i}"
            i += 1
        used.add(new)
        mapping[c] = new
    return df.rename(columns=mapping)

def sql_fetch_new_records(
    year: int,
    last_ts: str | None,
    last_id: int,
    page_size: int,
    max_pages: int,
    max_seconds: int,
    context
) -> list[dict]:
    """
    Fetch only rows for <year> that are newer than last_ts using datastore_search_sql.
    Watermark is string-based (YYYY-MM-DDTHH:MM:SS), which is lexicographically sortable.
    """
    start_ts = datetime.now(timezone.utc)
    offset = 0
    pages = 0
    out: list[dict] = []

    year_prefix = f"{year}-"

    while True:
        elapsed = (datetime.now(timezone.utc) - start_ts).total_seconds()
        if elapsed >= max_seconds:
            log_event("warning", {
                "event": "sql_paging_stopped",
                "dataset": DATASET_NAME,
                "reason": "max_seconds",
                "elapsed_seconds": int(elapsed),
                "pages": pages,
                "offset": offset,
                "records_accumulated": len(out),
            })
            break

        if context is not None:
            remaining_ms = context.get_remaining_time_in_millis()
            if remaining_ms is not None and remaining_ms < 15000:
                log_event("warning", {
                    "event": "sql_paging_stopped",
                    "dataset": DATASET_NAME,
                    "reason": "low_remaining_lambda_time",
                    "remaining_ms": int(remaining_ms),
                    "pages": pages,
                    "offset": offset,
                    "records_accumulated": len(out),
                })
                break

        if pages >= max_pages:
            log_event("warning", {
                "event": "sql_paging_stopped",
                "dataset": DATASET_NAME,
                "reason": "max_pages",
                "max_pages": max_pages,
                "pages": pages,
                "offset": offset,
                "records_accumulated": len(out),
            })
            break

        if last_ts:
            sql = (
                f'SELECT * FROM "{RESOURCE_ID}" '
                f"WHERE \"MessungDatZeit\" LIKE '{year_prefix}%' AND ("
                f"\"MessungDatZeit\" > '{last_ts}' OR "
                f"(\"MessungDatZeit\" = '{last_ts}' AND \"_id\" > {int(last_id)})"
                f") "
                f'ORDER BY "MessungDatZeit" ASC, "_id" ASC '
                f"LIMIT {page_size} OFFSET {offset}"
            )
        else:
            sql = (
                f'SELECT * FROM "{RESOURCE_ID}" '
                f"WHERE \"MessungDatZeit\" LIKE '{year_prefix}%' "
                f'ORDER BY "MessungDatZeit" ASC, "_id" ASC '
                f"LIMIT {page_size} OFFSET {offset}"
            )


        url = f"{API_ENDPOINT_SQL}?sql={urllib.parse.quote_plus(sql)}"
        with urllib.request.urlopen(url, timeout=120) as response:
            api_response = json.loads(response.read())

        records = (api_response.get("result", {}) or {}).get("records", []) or []

        log_event("info", {
            "event": "sql_page_fetched",
            "dataset": DATASET_NAME,
            "year": year,
            "watermark": last_ts,
            "page_size": page_size,
            "offset": offset,
            "records_in_page": len(records),
            "records_accumulated": len(out) + len(records),
        })

        if not records:
            break

        out.extend(records)
        pages += 1
        offset += len(records)

        if len(records) < page_size:
            break

    return out


def records_to_csv_lines(records: list[dict], fieldnames: list[str], include_header: bool) -> str:
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fieldnames, extrasaction="ignore")
    if include_header:
        writer.writeheader()
    writer.writerows(records)
    return buf.getvalue()


def append_to_bronze_year_csv(year: int, new_records: list[dict]) -> str:
    """
    Bronze yearly CSV is raw backup. Keep raw + append only new rows.
    Returns s3://... location.
    """
    csv_key = f"{BRONZE_PREFIX}/{year}/zurich_{DATASET_NAME}_{year}.csv"
    csv_location = f"s3://{BUCKET_NAME}/{csv_key}"

    existing_text = s3_get_text_if_exists(BUCKET_NAME, csv_key)

    if existing_text:
        first_line = existing_text.splitlines()[0] if existing_text else ""
        existing_fieldnames = [h.strip() for h in first_line.split(",")] if first_line else list(new_records[0].keys())

        new_chunk = records_to_csv_lines(new_records, existing_fieldnames, include_header=False)

        combined = existing_text
        if combined and not combined.endswith("\n"):
            combined += "\n"
        combined += new_chunk

        s3_put_text(BUCKET_NAME, csv_key, combined, "text/csv")
    else:
        fieldnames = list(new_records[0].keys())
        full_text = records_to_csv_lines(new_records, fieldnames, include_header=True)
        s3_put_text(BUCKET_NAME, csv_key, full_text, "text/csv")

    return csv_location


def month_key(year: int, mm: str) -> tuple[str, str]:
    parquet_key = f"{SILVER_PREFIX}/{year}/{DATASET_NAME}_{mm}_{year}.parquet"
    parquet_path = f"s3://{BUCKET_NAME}/{parquet_key}"
    return parquet_key, parquet_path


def merge_and_write_month_parquet(year: int, mm: str, new_records: list[dict]) -> None:
    """
    Reads existing month parquet if present, appends new rows, dedupes, writes back.
    Columns are normalized to snake_case in Parquet only.
    """
    _, parquet_path = month_key(year, mm)

    df_new = pd.DataFrame.from_records(new_records)
    if df_new.empty:
        return

    df_new = normalize_columns_snake_case(df_new)

    try:
        df_old = wr.s3.read_parquet(parquet_path)
        df_old = normalize_columns_snake_case(df_old)
        df_all = pd.concat([df_old, df_new], ignore_index=True)
    except Exception:
        df_all = df_new

    if DEDUP_ON_ID and "_id" in df_all.columns:
        df_all = df_all.drop_duplicates(subset=["_id"], keep="last")
    else:
        df_all = df_all.drop_duplicates(keep="last")

    wr.s3.to_parquet(
        df=df_all,
        path=parquet_path,
        dataset=False,
        index=False,
        compression="snappy",
    )


def lambda_handler(event, context):
    ingested_at = utc_now_iso()
    year = datetime.now(timezone.utc).year
    if isinstance(event, dict) and isinstance(event.get("year"), int):
        year = event["year"]

    year_str = str(year)
    bronze_year_prefix = f"s3://{BUCKET_NAME}/{BRONZE_PREFIX}/{year}/"
    silver_year_prefix = f"s3://{BUCKET_NAME}/{SILVER_PREFIX}/{year}/"

    last_ts = None
    csv_s3_location = None

    try:
        log_event("info", {
            "event": "ingestion_start",
            "dataset": DATASET_NAME,
            "ingested_at": ingested_at,
            "year": year,
            "sql_page_size": SQL_PAGE_SIZE,
            "max_pages": MAX_PAGES,
            "max_seconds": MAX_SECONDS,
            "lambda_request_id": getattr(context, "aws_request_id", None),
            "function_name": getattr(context, "function_name", None),
        })

        state = read_state()
        year_state = (state.get(year_str) or {})
        last_ts = year_state.get("last_messung_ts")
        last_id = year_state.get("last_id", -1)  # default: -1 so first _id at that timestamp is included


        log_event("info", {
            "event": "watermark_loaded",
            "dataset": DATASET_NAME,
            "ingested_at": ingested_at,
            "year": year,
            "watermark": last_ts,
            "watermark_id": last_id,
        })

        new_records = sql_fetch_new_records(
            year=year,
            last_ts=last_ts,
            last_id=last_id,
            page_size=SQL_PAGE_SIZE,
            max_pages=MAX_PAGES,
            max_seconds=MAX_SECONDS,
            context=context,
        )

        if not new_records:
            log_event("info", {
                "event": "ingestion_no_new_data",
                "dataset": DATASET_NAME,
                "ingested_at": ingested_at,
                "year": year,
                "records": 0,
                "watermark": last_ts,
                "bronze_prefix": bronze_year_prefix,
                "silver_prefix": silver_year_prefix,
            })

            payload = standard_audit_payload(context, DATASET_NAME, ingested_at)
            payload.update({
                "status": "no_new_data",
                "records": 0,
                "year": year,
                "watermark": last_ts or "",
                "s3_location": silver_year_prefix,
                "message": "No new records found (SQL incremental)",
            })
            write_audit(payload)

            return {"statusCode": 204, "body": json.dumps({"message": "No new records found", "year": year})}

        # Watermark after this run (keep full seconds precision)
        def rec_key(r: dict) -> tuple[str, int]:
            ts = (r.get("MessungDatZeit") or "")[:19]
            try:
                rid = int(r.get("_id")) if r.get("_id") is not None else -1
            except Exception:
                rid = -1
            return (ts, rid)

        max_ts, max_id = max((rec_key(r) for r in new_records), default=(last_ts or "", int(last_id)))


        # Bronze yearly CSV: append only new rows (raw)
        csv_s3_location = append_to_bronze_year_csv(year, new_records)

        log_event("info", {
            "event": "bronze_csv_appended",
            "dataset": DATASET_NAME,
            "ingested_at": ingested_at,
            "year": year,
            "new_records": len(new_records),
            "s3_location": csv_s3_location,
        })

        # Silver parquet: update only affected months
        months_affected = sorted({
            (r.get("MessungDatZeit") or "")[5:7]
            for r in new_records
            if (r.get("MessungDatZeit") or "")[:10].startswith(f"{year}-")
        })
        months_affected = [m for m in months_affected if len(m) == 2 and m.isdigit()]

        for mm in months_affected:
            month_records = [r for r in new_records if ((r.get("MessungDatZeit") or "")[5:7] == mm)]
            merge_and_write_month_parquet(year, mm, month_records)

            _, parquet_path = month_key(year, mm)
            log_event("info", {
                "event": "monthly_parquet_updated",
                "dataset": DATASET_NAME,
                "ingested_at": ingested_at,
                "year": year,
                "month": mm,
                "records_appended": len(month_records),
                "s3_location": parquet_path,
            })

        # Update state
        state[year_str] = {
            "last_messung_ts": max_ts,
            "last_id": int(max_id),
            "updated_at_utc": utc_now_iso(),
        }
        write_state(state)

        log_event("info", {
            "event": "ingestion_success",
            "dataset": DATASET_NAME,
            "ingested_at": ingested_at,
            "year": year,
            "new_records": len(new_records),
            "months_affected": months_affected,
            "watermark_before": last_ts,
            "watermark_after": max_ts,
            "csv_snapshot": csv_s3_location,
            "bronze_prefix": bronze_year_prefix,
            "silver_prefix": silver_year_prefix,
            "state_key": STATE_KEY,
        })

        payload = standard_audit_payload(context, DATASET_NAME, ingested_at)
        payload.update({
            "status": "success",
            "records": int(len(new_records)),
            "year": year,
            "watermark": max_ts or "",
            "s3_location": silver_year_prefix,
            "message": json.dumps({
                "csv_snapshot": csv_s3_location,
                "months_affected": months_affected,
                "records_written": len(new_records),
            }),
        })
        write_audit(payload)

        return {"statusCode": 200, "body": json.dumps({"message": "Success", "year": year, "new_records": len(new_records)})}

    except Exception as e:
        log_event("error", {
            "event": "ingestion_error",
            "dataset": DATASET_NAME,
            "ingested_at": ingested_at,
            "year": year,
            "watermark": last_ts,
            "csv_snapshot": csv_s3_location,
            "silver_prefix": silver_year_prefix,
            "state_key": STATE_KEY,
            "error": str(e),
        })

        payload = standard_audit_payload(context, DATASET_NAME, ingested_at)
        payload.update({
            "status": "error",
            "records": 0,
            "year": year,
            "watermark": last_ts or "",
            "s3_location": silver_year_prefix,
            "error": str(e),
        })
        write_audit(payload)

        return {"statusCode": 500, "body": json.dumps({"message": f"Error: {str(e)}"})}
