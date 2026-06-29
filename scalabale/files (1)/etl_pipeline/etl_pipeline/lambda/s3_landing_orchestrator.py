"""
Lambda: s3_landing_orchestrator
================================
Triggered by S3 ObjectCreated events when raw files land in the bronze prefix.
Responsibilities:
  1. Validate the landed object (expected prefix, non-empty, header sniff).
  2. Route the file to the correct dataset partition in the bronze lakehouse.
  3. Decide compute target: small/normal volume -> Glue; very large -> EMR.
  4. Start the orchestration Step Function (which runs Glue/EMR then Snowflake).
  5. Emit structured logs + a CloudWatch custom metric, DLQ bad files.

Environment variables:
  STATE_MACHINE_ARN   Step Functions ARN of the ETL orchestration
  EMR_SIZE_THRESHOLD  byte size above which we route to EMR instead of Glue
  DLQ_PREFIX          s3 prefix to move invalid files into
  EXPECTED_DATASETS   comma list: customer,orders,product,prod_cat_tree,currency_conversion
"""

import json
import logging
import os
import urllib.parse

import boto3

# Structured JSON logging -> CloudWatch Logs. One line per event = queryable.
logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")
sfn = boto3.client("stepfunctions")
cw = boto3.client("cloudwatch")

STATE_MACHINE_ARN = os.environ["STATE_MACHINE_ARN"]
EMR_SIZE_THRESHOLD = int(os.environ.get("EMR_SIZE_THRESHOLD", str(50 * 1024**3)))  # 50 GB
DLQ_PREFIX = os.environ.get("DLQ_PREFIX", "quarantine/")
EXPECTED = set(os.environ.get(
    "EXPECTED_DATASETS",
    "customer,orders,product,prod_cat_tree,currency_conversion",
).split(","))


def _log(level: str, **kw):
    """Emit a single structured log line."""
    getattr(logger, level)(json.dumps({"app": "s3_landing_orchestrator", **kw}))


def _emit_metric(name: str, value: float, dataset: str):
    cw.put_metric_data(
        Namespace="ETLPipeline",
        MetricData=[{
            "MetricName": name,
            "Value": value,
            "Unit": "Count",
            "Dimensions": [{"Name": "Dataset", "Value": dataset}],
        }],
    )


def _dataset_from_key(key: str) -> str:
    # bronze/raw/<dataset>/<partition>/file.ext
    parts = key.split("/")
    return parts[2] if len(parts) > 2 else ""


def _move_to_dlq(bucket: str, key: str, reason: str):
    dest = f"{DLQ_PREFIX}{key}"
    s3.copy_object(Bucket=bucket, CopySource={"Bucket": bucket, "Key": key}, Key=dest)
    s3.delete_object(Bucket=bucket, Key=key)
    _log("error", event="quarantined", key=key, dest=dest, reason=reason)


def handler(event, context):
    started = []
    for record in event.get("Records", []):
        bucket = record["s3"]["bucket"]["name"]
        key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])
        size = record["s3"]["object"].get("size", 0)
        dataset = _dataset_from_key(key)

        _log("info", event="received", bucket=bucket, key=key, size=size, dataset=dataset)

        # --- validation -----------------------------------------------------
        if dataset not in EXPECTED:
            _move_to_dlq(bucket, key, f"unexpected_dataset:{dataset}")
            _emit_metric("InvalidFile", 1, dataset or "unknown")
            continue

        if size == 0:
            _move_to_dlq(bucket, key, "empty_file")
            _emit_metric("InvalidFile", 1, dataset)
            continue

        # cheap header sniff for CSV datasets (skip JSON)
        if not key.endswith(".json"):
            head = s3.get_object(Bucket=bucket, Key=key, Range="bytes=0-2048")
            first_line = head["Body"].read().decode("utf-8", "ignore").splitlines()[:1]
            if not first_line or "," not in first_line[0]:
                _move_to_dlq(bucket, key, "missing_or_malformed_header")
                _emit_metric("InvalidFile", 1, dataset)
                continue

        # --- routing decision: Glue vs EMR ----------------------------------
        compute = "emr" if size >= EMR_SIZE_THRESHOLD else "glue"
        _log("info", event="route", key=key, compute=compute, size=size)

        # --- start orchestration --------------------------------------------
        exec_input = {
            "bucket": bucket,
            "key": key,
            "dataset": dataset,
            "compute": compute,
            "run_date": context.aws_request_id[:8],
        }
        resp = sfn.start_execution(
            stateMachineArn=STATE_MACHINE_ARN,
            input=json.dumps(exec_input),
        )
        started.append(resp["executionArn"])
        _emit_metric("FileAccepted", 1, dataset)
        _log("info", event="execution_started", arn=resp["executionArn"], dataset=dataset)

    return {"started": started, "count": len(started)}
