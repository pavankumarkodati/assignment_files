# End-to-End ETL Pipeline — Design, Implementation & Scaling

AWS Glue · EMR · Lambda · Snowflake | batch + streaming lakehouse

---

## 1. What this solves

The source files describe a small retail dataset (customers, orders, products, a
category tree, and daily FX rates). The assignment is to clean and standardize
the data, build a dimensional model, and expose core business metrics. This
solution does that **and** answers the harder question you posed: how to keep the
exact same logic correct when records explode by **100x–1000x** and arrive as
either **batch files or a live stream**.

The design choice that makes this possible: separate *ingestion* from *transform*
from *serving*, land everything in an S3 lakehouse in a single canonical layout,
and run the same PySpark logic on Glue (normal volume) or EMR (heavy volume)
without rewriting it.

---

## 2. Architecture (layer by layer)

**Ingestion.** Two doors, one room.
- *Batch:* source files land in `s3://lakehouse/bronze/raw/<dataset>/`. An S3
  `ObjectCreated` event invokes the `s3_landing_orchestrator` Lambda, which
  validates the file (expected dataset, non-empty, header sniff), routes bad
  files to a quarantine prefix, decides Glue-vs-EMR by size, and starts the
  Step Functions execution.
- *Stream:* live order events flow through Kinesis Data Streams. The
  `stream_order_validator` Lambda applies the *same* row-level rules as the batch
  job, then writes good records via Firehose into the *same* bronze layout. The
  stream and batch paths therefore converge — there is one set of semantics, not
  two.

**Lakehouse on S3 (Apache Iceberg).** Three medallion zones:
- *Bronze* — raw, immutable, exactly as received.
- *Silver* — cleansed, conformed, deduped, currency-normalized; SCD2 customer.
- *Gold* — the dimensional star schema, partitioned by `order_day`.

Iceberg gives ACID writes, `MERGE` for upserts/SCD2, schema evolution, hidden
partitioning, and time travel — all of which matter once files number in the
millions.

**Compute.** Glue runs the standard PySpark job (`bronze_to_silver.py`). When the
orchestrator sees a very large landing, it routes the identical script to a
transient EMR cluster with Spot task nodes. Same code, bigger engine.

**Serving (Snowflake).** Snowpipe auto-ingests new Gold Parquet files the moment
they appear (S3 → SNS/SQS → Snowpipe), giving near-real-time loading with no
warehouse running. Streams capture only the new rows; Tasks incrementally append
facts and `MERGE` the SCD2 customer dimension every minute. BI runs on a separate
multi-cluster warehouse so dashboards never contend with loading.

**Orchestration & observability.** Step Functions coordinates Glue/EMR → Snowflake
with retries, catch states, and SNS failure alerts (Airflow/MWAA is a drop-in
alternative for richer DAGs). Every Lambda and Spark stage emits structured
single-line JSON logs to CloudWatch plus custom metrics (`FileAccepted`,
`InvalidFile`, `StreamRecordsGood/Bad`), so the whole pipeline is queryable in
CloudWatch Insights and alarmable.

---

## 3. Data model (star schema)

Grain of the fact table is **one order line item**.

| Table | Type | Key | Notes |
|---|---|---|---|
| `fact_orders` | fact | order_id + prod_id + order_ts | revenue_usd, quantity, status, is_revenue_status, FX-converted |
| `dim_customer` | SCD Type 2 | customer_sk | valid_from / valid_to / is_current; region & active change over time |
| `dim_product` | dimension | prod_id | currency normalized; category_name + root_category attached |
| `dim_category` | dimension | cat_id | recursive tree flattened to a root label (Sweet / salt) |
| `dim_date` | dimension | date_key | calendar attributes for BI |
| `orders_quarantine` | audit | — | every rejected row + reason |

**Mapping / transformation highlights**
- `order date` → `order_ts` (parsed `M/d/yyyy H:mm`) and `order_day`.
- `order_id` normalized: `A-21` → `A-021` so IDs sort and join consistently.
- product `currency` upper-cased: `Yen` → `YEN`, enabling the FX join.
- `revenue_usd = price × quantity × conversion`, where `conversion` is the daily
  rate for the order's date and currency (nearest-prior rate as fallback).
- category tree resolved by iterative parent-walk; `Sweet` = cat_id 1, 2, 3.
- revenue recognized only for `status ∈ {paid, shipped}` (`created`/`cancelled`
  excluded via `is_revenue_status`).

---

## 4. Data-quality issues found & how they're handled

Every issue below was confirmed by running the transform against the real files.

| Issue | Example | Handling |
|---|---|---|
| Exact duplicate order lines | `A-005` appears 3× identical | Dedup on full grain (order, product, ts, qty, status); legitimate multi-line orders like `A-009` survive |
| Orphan orders (null customer) | `A-21`, `A-22` | Quarantined as `orphan_customer` |
| Fractional quantity | `A-013` qty `0.1` | Quarantined as `fractional_quantity` |
| Inconsistent order IDs | `A-001` vs `A-21` | Zero-padded to `A-NNN` |
| Currency casing mismatch | product `Yen` vs FX `YEN` | Upper-case + trim both sides before join |
| Customer SCD | same `cust_id`, changing `active` | SCD2 with valid_from/valid_to/is_current |
| Recursive category tree | Sweet→chocolate→… | Iterative flatten to root label |
| Status not equal to revenue | `created`, `cancelled` | `is_revenue_status` flag, excluded from revenue |

Nothing is silently dropped — faulty rows land in `orders_quarantine` with a
reason, so they're auditable in BI (assignment metric *e*).

**Verified output** (real data): 24 raw order rows → 3 quarantined → 21 clean →
19 after dedup. Sweet category: ~$597.86 revenue across 6 realized of 8 total
orders, 17 units. Top product by revenue: `kjhhjk` (`A-018`, 20 units YEN).

---

## 5. Trade-offs & what changes at 100x / 1000x

**What I simplified for the exercise**
- The nearest-prior FX fallback uses a windowed join; fine at this size, but it's
  a range/as-of join that needs care at scale (below).
- `dim_date` is assumed pre-populated; in production it's generated once.
- Single region / no late-arriving-data reconciliation window modeled here.

**What I would change for 100x–1000x**

*Storage & layout.* Partition Gold by `order_day` (already done) and compact small
files — the #1 killer at scale is millions of tiny Parquet files from streaming
micro-batches. Run Iceberg `rewrite_data_files` compaction on a schedule. Use
Iceberg hidden partitioning so queries prune without users knowing the layout.

*Skew.* `A-005`-style hot keys become real skew at scale. Spark AQE skew-join is
enabled; for pathological keys, salt the join key. Broadcast the small dimensions
(`dim_product`, `dim_category`, FX rates) so the large fact never shuffles against
them.

*Compute routing.* The orchestrator already switches Glue → EMR by file size.
Glue is great for spiky, unpredictable volume (serverless, per-DPU). EMR with
Spot task fleets is cheaper for sustained 1000x reprocessing. The same
`bronze_to_silver.py` runs on both — no rewrite.

*Streaming throughput.* Kinesis scales by shard; size shards to peak event rate
and use enhanced fan-out if multiple consumers. Firehose dynamic partitioning
writes directly into the dated bronze layout. For very high volume, replace the
Snowpipe-file path with **Snowpipe Streaming** for sub-second, rowset ingestion.

*Snowflake.* Cluster `fact_orders` by `order_day` for pruning; use a multi-cluster
BI warehouse for concurrency and a dedicated load warehouse. Streams + Tasks
process only deltas, so cost scales with *new* data, not total. Consider Dynamic
Tables to declare the incremental transforms instead of hand-written Tasks.

*FX join at scale.* Replace the windowed nearest-prior with a broadcasted,
pre-filled daily-rate table (forward-fill gaps once, then a plain equi-join on
date+currency) — O(n) instead of a window over the full fact.

*Reliability.* Idempotent loads (anti-join / `MERGE` on natural keys) so replayed
Snowpipe files or Kinesis re-deliveries never double-count. DLQs on both Lambdas.
Step Functions retries with backoff. Glue Data Quality (DQDL) or Great
Expectations gate the Silver boundary and fail loudly.

---

## 6. Repository contents

```
etl_pipeline/
├── glue_jobs/
│   └── bronze_to_silver.py          # clean, dedup, SCD2, currency, category — verified
├── lambda/
│   ├── s3_landing_orchestrator.py   # batch: validate + route + start Step Function
│   └── stream_order_validator.py    # stream: per-event validation -> Firehose -> bronze
├── emr/
│   └── emr_cluster_config.json      # transient Spot cluster for 1000x batch
├── orchestration/
│   └── etl_state_machine.asl.json   # Glue/EMR -> Snowflake, retries + alerts
├── snowflake/
│   ├── 01_setup_and_snowpipe.sql    # warehouses, stage, Snowpipe auto-ingest
│   ├── 02_model_streams_tasks.sql   # star schema + incremental SCD2 via Streams/Tasks
│   └── 03_business_metrics.sql      # the 5 assignment queries
└── docs/
    └── DESIGN.md                    # this document
```

## 7. AI tool disclosure

Per the assignment's open-book note: the architecture, code, and this write-up
were drafted with Claude. The core transformation logic in `bronze_to_silver.py`
was executed against the actual provided source files to confirm the dedup,
quarantine, currency, and category-rollup behavior described in section 4.
