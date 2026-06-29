"""
Lambda: stream_order_validator
===============================
The streaming path. Subscribed to a Kinesis Data Stream of live order events.
For each record it applies the SAME validation rules as the batch Glue job so the
stream and batch paths converge on identical semantics (a "kappa-ish" design):

  * reject orphan customers, non-positive / fractional quantities, bad timestamps
  * normalize order_id (A-21 -> A-021), lower-case status
  * forward GOOD records to Firehose -> S3 bronze (micro-batched, same layout as
    batch landings, so the Glue/EMR silver job treats them uniformly)
  * forward BAD records to a Firehose DLQ stream + CloudWatch metric

This keeps per-event latency low while letting heavy transforms (currency join,
SCD2, dedup across the whole table) happen downstream in Glue/Snowflake where
they belong. Kinesis batches up to ~10k records per invocation; we scale by
adding shards (see docs/scaling.md).
"""

import base64
import json
import logging
import os
import re
from datetime import datetime

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

firehose = boto3.client("firehose")
cw = boto3.client("cloudwatch")

GOOD_STREAM = os.environ["FIREHOSE_GOOD"]      # -> s3 bronze/raw/orders/
DLQ_STREAM = os.environ["FIREHOSE_DLQ"]        # -> s3 quarantine/orders/
_ORDER_ID_RE = re.compile(r"^([A-Za-z]+)-(\d+)$")


def _log(level, **kw):
    getattr(logger, level)(json.dumps({"app": "stream_order_validator", **kw}))


def _normalize_order_id(oid: str):
    m = _ORDER_ID_RE.match((oid or "").strip())
    if not m:
        return None
    return f"{m.group(1)}-{int(m.group(2)):03d}"


def _validate(rec: dict):
    """Return (clean_dict, None) if valid, else (None, reason)."""
    cust = (rec.get("cust_id") or "").strip()
    if not cust:
        return None, "orphan_customer"

    try:
        qty = float(rec.get("quantity"))
    except (TypeError, ValueError):
        return None, "bad_quantity"
    if qty <= 0:
        return None, "non_positive_quantity"
    if qty != int(qty):
        return None, "fractional_quantity"

    try:
        ts = datetime.strptime(rec["order_date"], "%m/%d/%Y %H:%M")
    except (KeyError, ValueError):
        return None, "bad_timestamp"

    oid = _normalize_order_id(rec.get("order_id"))
    if oid is None:
        return None, "bad_order_id"

    return {
        "order_id": oid,
        "cust_id": cust,
        "order_date": ts.strftime("%-m/%-d/%Y %H:%M"),
        "prod_id": int(rec["prod_id"]),
        "quantity": int(qty),
        "status": (rec.get("status") or "").strip().lower(),
    }, None


def _put(stream: str, payload: dict):
    firehose.put_record(
        DeliveryStreamName=stream,
        Record={"Data": (json.dumps(payload) + "\n").encode("utf-8")},
    )


def handler(event, context):
    good = bad = 0
    for record in event.get("Records", []):
        try:
            raw = base64.b64decode(record["kinesis"]["data"])
            rec = json.loads(raw)
        except Exception as e:  # noqa: BLE001 — malformed envelope -> DLQ
            _put(DLQ_STREAM, {"raw": raw.decode("utf-8", "ignore"), "reason": f"decode:{e}"})
            bad += 1
            continue

        clean, reason = _validate(rec)
        if reason:
            _put(DLQ_STREAM, {**rec, "dq_reason": reason})
            bad += 1
            _log("warning", event="rejected", order_id=rec.get("order_id"), reason=reason)
        else:
            _put(GOOD_STREAM, clean)
            good += 1

    cw.put_metric_data(
        Namespace="ETLPipeline",
        MetricData=[
            {"MetricName": "StreamRecordsGood", "Value": good, "Unit": "Count"},
            {"MetricName": "StreamRecordsBad", "Value": bad, "Unit": "Count"},
        ],
    )
    _log("info", event="batch_done", good=good, bad=bad)
    return {"good": good, "bad": bad}
