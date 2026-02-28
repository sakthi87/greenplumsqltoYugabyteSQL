# GreenPlum SP to YugabyteDB (Spark COPY)

This project runs a SQL file against YugabyteDB, then streams the result into
another YugabyteDB table using `COPY FROM STDIN` with `REPLACE`. It is intended
as a generic job for converting stored procedures into optimized `SELECT`
queries and loading the output efficiently.

## What this includes

- A Spark job that reads an optimized SQL file via JDBC
- Streaming COPY writer (no local files)
- `COPY ... WITH (REPLACE)` for idempotent upsert
- A sample optimized SQL for `tbl_entityres_card_merchants`

## How to run

1. Edit `src/main/resources/job.properties`
2. Build the job:

```
mvn package
```

3. Run the job with Spark:

```
spark-submit \
  --class com.yb.migration.QueryCopyJob \
  target/yb-spark-copy-job-1.0.0.jar \
  /path/to/job.properties \
  /path/to/sql/entityres_card_merchants.sql
```

## SQL placeholders

The SQL file supports two placeholders:

- `{{start_date}}`
- `{{end_date}}`

Update them via `job.properties`.

## Optional batching

Enable built-in batching (daily/weekly) by setting:

```
batch.enabled=true
batch.window_days=1
batch.max_batches=0
```

When enabled, the job loops from `sql.start_date` to `sql.end_date` in
`batch.window_days` increments. `batch.max_batches=0` means no limit.

## Optional JDBC partitioning (parallel reads)

Spark can read in parallel when these are set:

```
jdbc.partition.column=transaction_posted_dt
jdbc.partition.lower=2026-01-01
jdbc.partition.upper=2026-12-31
jdbc.partition.num=64
```

Partitioning is only used when all values are set and `num > 1`.

## Examples

Weekly batching:

```
batch.enabled=true
batch.window_days=7
batch.max_batches=0
```

Suggested partition counts:

- 10M–100M rows: `jdbc.partition.num=16` to `32`
- 100M–500M rows: `jdbc.partition.num=32` to `64`
- 500M+ rows: `jdbc.partition.num=64` to `128`

## Next steps

- Add more SQL files and run them with the same job.
- Wrap this into a driver script to iterate over 100s of jobs.
