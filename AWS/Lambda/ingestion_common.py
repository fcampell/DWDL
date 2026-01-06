import os
import json
import logging
from datetime import datetime, timezone
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def log_event(level: str, payload: dict) -> None:
    msg = json.dumps(payload, ensure_ascii=False)
    if level == "error":
        logger.error(msg)
    elif level == "warning":
        logger.warning(msg)
    else:
        logger.info(msg)


def audit_writer(dynamodb_resource, dataset_name: str, audit_table: str):
    table = dynamodb_resource.Table(audit_table)

    def write(payload: dict) -> None:
        try:
            table.put_item(Item=payload)
        except Exception as e:
            log_event("warning", {"event": "audit_write_failed", "dataset": dataset_name, "error": str(e)})

    return write


def s3_state_reader_writer(s3_client, bucket: str, state_key: str):
    def read_state() -> dict:
        try:
            obj = s3_client.get_object(Bucket=bucket, Key=state_key)
            return json.loads(obj["Body"].read().decode("utf-8"))
        except ClientError as e:
            code = (e.response.get("Error", {}) or {}).get("Code")
            if code in ("NoSuchKey", "404"):
                return {}
            raise

    def write_state(state: dict) -> None:
        s3_client.put_object(
            Bucket=bucket,
            Key=state_key,
            Body=json.dumps(state, indent=2).encode("utf-8"),
            ContentType="application/json",
        )

    return read_state, write_state


def standard_audit_payload(context, dataset: str, ingested_at: str) -> dict:
    return {
        "dataset": dataset,
        "ingested_at": ingested_at,
        "function_name": getattr(context, "function_name", ""),
        "request_id": getattr(context, "aws_request_id", ""),
    }
