## Summary of the Stored Procedure

This section breaks the original function into its logical parts and describes what each part does.

### 1) Prolog, metadata, and run notes

- Declares timestamps and counters for auditing (`initial_ts`, `final_ts`, `initial_record_count`, `final_record_count`).
- Defines job metadata (`job_name`, `run_status`, `audit_text`).
- Includes comments for author, dates, and change history.
- Provides a sample call for executing the function.

### 2) Runtime notifications and initial metrics

- Emits runtime notices with `clock_timestamp()` to track execution progress.
- Captures `initial_ts` and the existing row count in `tbl_entityres_card_merchants`.

### 3) Main insert into `tbl_entityres_card_merchants`

- Builds a composite `party_id` using a `concat(...)` of merchant attributes.
- Inserts aggregated metrics from `staging_cust360_corp_credit_card_trans`:
  - `COUNT(DISTINCT business_card_account_nbr)` as counterparty count
  - `SUM(usd_amt)` as total USD amount
  - `COUNT(*)` as transaction count
  - `MAX(transaction_posted_dt)` as last transaction date
- Uses a `LEFT JOIN` to avoid inserting rows already in the target table.
- Uses `NOT IN (SELECT DISTINCT record_load_date ...)` to avoid reprocessing existing load dates.
- Applies basic filters on merchant fields (invalid merchant number / SIC code).

### 4) Update existing rows with refreshed metrics

- Recomputes the same aggregates across the entire staging table.
- Updates the target table to refresh:
  - `counter_party_cnt`
  - `total_usd_amount`
  - `total_tnx_count`
  - `last_tnx_date`

### 5) Update `first_occur_date`

- Computes `MIN(transaction_posted_dt)` across staging per party.
- Updates `first_occur_date` only where it is currently `NULL`.

### 6) Final metrics, audit logging, and error handling

- Recomputes final row count and derives the count delta.
- Records `final_ts` and writes to the audit log table.
- On error, logs the failure status with error information.

---

## Proposed Solution (Full Refresh)

### Summary (bullet points)

- Replace the multi-step SP with a single optimized aggregation query.
- Use Spark to run the query and stream results directly into YugabyteDB.
- Load results via `COPY FROM STDIN WITH (REPLACE)` for idempotent refresh.
- Remove the expensive `LEFT JOIN`, `NOT IN`, and follow-up `UPDATE` scans.
- Enable optional batching and JDBC partitioning for scalable execution.

### Detailed Steps and Explanations

#### Step 1: Replace the SP with a single full-refresh aggregation

**Why:** The original SP performs three full passes over a 500M row staging table and uses a join on `concat(...)`, which prevents index use and causes long-running scans.  
**What changes:** We compute the required metrics once and return the full refreshed dataset.

**Key effects:**
- Eliminates the anti-join on `party_id`.
- Eliminates the `NOT IN` record_load_date check.
- Eliminates the two `UPDATE` steps.
- Preserves final metric correctness for a full refresh.

#### Step 2: Stream results into the target table using COPY REPLACE

**Why:** `COPY FROM STDIN WITH (REPLACE)` provides fast bulk ingestion and deterministic replacement on primary key.  
**What changes:** We insert the full refreshed dataset from Spark with COPY (no local files).

**Result:** A single idempotent load that replaces existing rows for a given `party_id`.

#### Step 3: Optional batching for large datasets

**Why:** If the full refresh dataset is too large, batch by date to avoid massive scans and long-running tasks.  
**How:** Enable built-in looping with:

```
batch.enabled=true
batch.window_days=1
batch.max_batches=0
```

Each batch renders SQL with `{{start_date}}` and `{{end_date}}`.

#### Step 4: Optional JDBC partitioning for parallel reads

**Why:** Partitioned reads accelerate large scans.  
**How:** Configure:

```
jdbc.partition.column=transaction_posted_dt
jdbc.partition.lower=2026-01-01
jdbc.partition.upper=2026-12-31
jdbc.partition.num=64
```

The job will split the query into multiple predicates and read them in parallel.

#### Step 5: Operational workflow

**Run flow:**
- Update `job.properties` with JDBC credentials and dates.
- Run Spark job with SQL file.
- Spark runs the optimized query and streams into Yugabyte via COPY.

**Expected outcome:**
- Far fewer scans and no expensive joins.
- Stable runtime and improved throughput.
- No need for the post-insert update steps.

