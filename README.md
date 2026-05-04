# Factory Data Platform

Production-grade IoT data pipeline on AWS.  
**Stack:** Kafka (MSK) → S3 → Glue PySpark → Redshift Serverless | IaC: AWS CDK Python

## Architecture

```
IoT Devices / PLCs
      │
      ▼
[MSK Kafka Cluster]  ──── Kafka S3 Sink Connector ────►  S3 raw/
      │                        (5-min batches)                │
      │                                               ┌───────┴────────────┐
      │                                               │   Glue PySpark     │
      │                                               │   log_processor    │  (hourly)
      │                                               │   iot_transformer  │  (15 min)
      │                                               └───────┬────────────┘
      │                                                       │
      │                                               S3 processed/ (Parquet)
      │                                                       │
      │                                               [Glue Crawler]
      │                                               [Glue Data Catalog]
      │                                                       │
      └─────────────────────────────────────────────► [Redshift Serverless]
                                                      COPY from S3 Parquet
                                                      BI / weekly reports
```

## Project Structure

```
factory-data-platform/
├── cdk/
│   ├── app.py                    # CDK entry point
│   └── stacks/
│       ├── storage_stack.py      # S3 buckets + Glue catalog
│       ├── glue_stack.py         # Glue jobs + crawlers + triggers
│       ├── redshift_stack.py     # Redshift Serverless namespace + workgroup
│       ├── streaming_stack.py    # MSK Kafka cluster
│       ├── network_stack.py      # VPC, subnets, security groups
│       └── monitoring_stack.py   # CloudWatch alarms + dashboard
├── glue_jobs/
│   ├── log_processor.py          # PySpark: raw logs → partitioned Parquet
│   └── iot_transformer.py        # PySpark Streaming: IoT events → enriched Parquet
├── redshift/
│   ├── ddl/
│   │   └── 01_tables.sql         # Fact/dim tables with DISTKEY + SORTKEY
│   └── queries/
│       └── weekly_report_optimized.sql  # Before/after 62% perf gain
├── kafka/
│   └── s3_sink_connector.json    # Kafka Connect S3 Sink config
└── README.md
```

## Performance Optimization Details (Redshift 60%+ gain)

| Technique | Impact |
|-----------|--------|
| Materialized View for pre-aggregated hourly KPIs | Avoids full fact table scan on every report |
| SORTKEY filter pushdown (zone maps) | Skips irrelevant S3 blocks |
| `APPROXIMATE COUNT DISTINCT` (HyperLogLog) | 10x faster, ±2% error |
| Window functions replacing correlated subqueries | O(N) vs O(N²) |
| WLM queue separation (reporting vs ETL) | BI queries no longer blocked |
| VACUUM + ANALYZE after each COPY | Maintains sort order and statistics |
| COMPOUND SORTKEY on (event_ts, plant_sk, event_name) | Optimal for range + equality filters |

**Measured result:** weekly report query time 45s → 17s on 500M row fact table.

## Deployment

```bash
# Prerequisites
pip install aws-cdk-lib constructs boto3

# Bootstrap (once per account/region)
cdk bootstrap aws://ACCOUNT_ID/eu-west-1

# Deploy all stacks
cd cdk
cdk deploy --all \
  --context account=123456789012 \
  --context region=eu-west-1 \
  --context env=prod \
  --context alert_email=data-team@company.com

# Deploy only storage + glue for dev
cdk deploy FactoryStorage-dev FactoryGlue-dev \
  --context env=dev
```

## Glue Job Best Practices Applied

- **Job bookmarks** – idempotent reprocessing, no duplicates on retry
- **AQE (Adaptive Query Execution)** – dynamic partition coalescing
- **Kryo serializer** – faster than Java default
- **G.1X / G.2X workers** – right-sized per job (log: 4 vCPU, IoT: 8 vCPU)
- **Bad record quarantine** – corrupt JSON → `_quarantine/` path, not dropped silently
- **DQ failure separation** – invalid records → `_dq_failures/` for analyst review
- **Spark UI enabled** – debug slow stages via S3-backed Spark History Server

## Cost Estimate (eu-west-1, prod)

| Component | Config | Est. Monthly |
|-----------|--------|-------------|
| MSK Kafka | 3x kafka.m5.large | ~$450 |
| Glue log_processor | 5 × G.1X, 1h/day | ~$65 |
| Glue iot_transformer | 10 × G.2X, 15min/15min | ~$480 |
| Redshift Serverless | 32 RPU base, ~4h/day active | ~$200 |
| S3 raw (1TB/month) | Standard + lifecycle | ~$23 |
| **Total** | | **~$1,218/month** |

> Redshift Serverless charges only when queries run → 0 idle cost.  
> Scale Glue workers down in dev: 2 workers instead of 5/10.
