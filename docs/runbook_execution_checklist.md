## Spark Execution Checklist

### 1) Prerequisites
- Confirm Java 11+ and Spark 3.x are installed.
- Verify network access from Spark nodes to YugabyteDB.
- Confirm JDBC credentials for source and target.
- Ensure the SQL file exists on the Spark submit host.
- Decide full refresh window (`sql.start_date`, `sql.end_date`).

### 2) Sandbox table creation (optional but recommended)
- Create sandbox copies so production tables are not touched:

```
psql -h <yb_host> -p 5433 -U <user> -d <db> -f sql/setup_duplicate_tables.sql
```

### 3) Add and populate `party_id` (optional, recommended for 1-hour target)
- If using sandbox tables, apply these to the sandbox staging table.

```
ALTER TABLE moneymovement_source.staging_cust360_corp_credit_card_trans_sandbox
ADD COLUMN party_id text;

UPDATE moneymovement_source.staging_cust360_corp_credit_card_trans_sandbox
SET party_id = concat_ws('|', acquirer_member_id, merchant_ica_nbr, mvv_id,
  merchant_nbr, merchant_nm, merchant_city_nm, merchant_state_nm,
  merchant_postal_cde, merchant_country_cde, merchant_sic_cde);

CREATE INDEX ON moneymovement_source.staging_cust360_corp_credit_card_trans_sandbox (party_id);
```

### 4) Update job properties
- If using sandbox tables, set:
  - `target.table=tbl_entityres_card_merchants_sandbox`
- Choose the SQL file:
  - `sql/entityres_card_merchants_sandbox.sql`
  - or `sql/entityres_card_merchants_party_id.sql` (if `party_id` is precomputed)

### 5) Run Spark submit
- Build or use the provided jar.
- Run `spark-submit` with job properties and SQL file:

```
spark-submit \
  --class com.yb.migration.QueryCopyJob \
  /path/to/yb-spark-copy-job-1.0.0.jar \
  /path/to/job.properties \
  /path/to/sql/entityres_card_merchants.sql
```

---

### 6) Performance tuning (Standalone Spark)

### 1) JDBC partitioning: how to pick bounds

Use these queries to find a safe range for `transaction_posted_dt`:

```
SELECT
  MIN(transaction_posted_dt) AS min_dt,
  MAX(transaction_posted_dt) AS max_dt
FROM moneymovement_source.staging_cust360_corp_credit_card_trans
WHERE merchant_nbr IS NOT NULL
  AND merchant_nbr <> '000000000000000'
  AND merchant_sic_cde <> '0000';
```

If the date distribution is skewed, check row counts per month:

```
SELECT date_trunc('month', transaction_posted_dt) AS month, COUNT(*) AS rows
FROM moneymovement_source.staging_cust360_corp_credit_card_trans
WHERE merchant_nbr IS NOT NULL
  AND merchant_nbr <> '000000000000000'
  AND merchant_sic_cde <> '0000'
GROUP BY 1
ORDER BY 1;
```

Then set:

```
jdbc.partition.column=transaction_posted_dt
jdbc.partition.lower=<min_dt>
jdbc.partition.upper=<max_dt>
jdbc.partition.num=64
```

### 2) Spark parallelism: sizing for a 1-hour window

Your target is ~1 hour. With your current 3-worker layout, you can hit higher
core counts by resizing workers:

- 3 workers × 16 vCPU = **48 total cores**
- 3 workers × 32 vCPU = **96 total cores**

Either option can support a 1-hour goal, but 32 vCPU workers give more headroom.

### 3) Executor CPU vs memory sizing (Standalone)

Recommended starting point per executor:

- 6–8 cores
- 24–28 GB memory

If you run **one executor per worker**, use:

```
--executor-cores 8
--executor-memory 24g
```

If you run **two executors per worker**, use:

```
--executor-cores 4
--executor-memory 12g
```

Leave ~4–6 GB per worker for OS and JVM overhead.

#### Suggested layouts for 1-hour target

**Option A: 3 workers × 16 vCPU (48 cores total)**

- Use 2 executors per worker
- Each executor: 8 cores, 24 GB memory
- Total executors: 6
- Total executor cores: 48

**Spark submit flags:**

```
--num-executors 6
--executor-cores 8
--executor-memory 24g
```

**Option B: 3 workers × 32 vCPU (96 cores total)**

- Use 3 executors per worker
- Each executor: 10–12 cores, 28–30 GB memory
- Total executors: 9
- Total executor cores: 90–108

**Spark submit flags (start with 10 cores):**

```
--num-executors 9
--executor-cores 10
--executor-memory 28g
```

Monitor GC and reduce cores if you see long GC pauses.

### 3) Spark configuration flags (recommended)

Use these as a starting point for large aggregation jobs:

```
--conf spark.sql.shuffle.partitions=512
--conf spark.default.parallelism=512
--conf spark.sql.adaptive.enabled=true
--conf spark.sql.adaptive.coalescePartitions.enabled=true
--conf spark.sql.adaptive.skewJoin.enabled=true
--conf spark.executor.heartbeatInterval=60s
--conf spark.network.timeout=800s
```

Tuning guidance:

- If you have 48 total cores, start with `shuffle.partitions=384` to `512`.
- If you have 96 total cores, start with `shuffle.partitions=768` to `1024`.
- If you see excessive small tasks, lower `shuffle.partitions`.
- If tasks are too large/slow, increase `shuffle.partitions`.

### 4) Batching for full refresh (important caveat)

Batching by **date** does **not** produce a correct full refresh for this
aggregation because each `party_id` can appear in multiple date windows.
With `COPY ... REPLACE`, the **last batch wins**, which overwrites earlier
aggregates and produces **partial totals**.

Use batching only if you also change the workflow to:

- `COPY` into a **staging table**, then
- run a **final merge/aggregate** across all staging batches into the target.

If you keep the current single-step full refresh, **do not enable batching**.

### 5) Precompute `party_id` (recommended for 1-hour target)

If you can change the staging table, precompute the key once:

```
ALTER TABLE moneymovement_source.staging_cust360_corp_credit_card_trans
ADD COLUMN party_id text;

UPDATE moneymovement_source.staging_cust360_corp_credit_card_trans
SET party_id = concat_ws('|', acquirer_member_id, merchant_ica_nbr, mvv_id,
  merchant_nbr, merchant_nm, merchant_city_nm, merchant_state_nm,
  merchant_postal_cde, merchant_country_cde, merchant_sic_cde);

CREATE INDEX ON moneymovement_source.staging_cust360_corp_credit_card_trans (party_id);
```

Then update the SQL file to use `party_id` directly to reduce CPU per row.

### 6) Yugabyte session parameters (safe defaults)

Set these in the Spark job session (already done for COPY):

- `SET yb_disable_transactional_writes = on;`

For heavy read aggregation (optional per session):

- `SET yb_enable_parallel_append = on;`
- `SET max_parallel_workers_per_gather = 8;`
- `SET max_parallel_workers = 8;`
- `SET work_mem = '128MB';` (adjust if you see sorts spilling)

Use these only for the Spark JDBC session, not globally.

### 7) Post-run validation
- Row count check:

```
SELECT COUNT(*) FROM moneymovement_source.tbl_entityres_card_merchants;
```

- Aggregate sanity check:

```
SELECT
  COUNT(*) AS tgt_rows,
  SUM(total_usd_amount) AS tgt_total_usd,
  SUM(total_tnx_count) AS tgt_total_tnx
FROM moneymovement_source.tbl_entityres_card_merchants;
```

- Compare with staging:

```
SELECT
  COUNT(DISTINCT concat_ws('|', acquirer_member_id, merchant_ica_nbr, mvv_id,
    merchant_nbr, merchant_nm, merchant_city_nm, merchant_state_nm,
    merchant_postal_cde, merchant_country_cde, merchant_sic_cde)) AS expected_rows,
  SUM(usd_amt) AS expected_total_usd,
  COUNT(*) AS expected_total_tnx
FROM moneymovement_source.staging_cust360_corp_credit_card_trans
WHERE merchant_nbr IS NOT NULL
  AND merchant_nbr <> '000000000000000'
  AND merchant_sic_cde <> '0000';
```

- Date checks:

```
SELECT
  MIN(first_occur_date) AS min_first_occur,
  MAX(last_tnx_date) AS max_last_tnx
FROM moneymovement_source.tbl_entityres_card_merchants;
```
