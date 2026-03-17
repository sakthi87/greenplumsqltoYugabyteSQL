## Spark submit (with driver on classpath)

Use this command to avoid "No suitable driver" errors:

```
nohup spark-submit \
  --class com.yb.migration.QueryCopyJob \
  --jars /opt/jdbc/jdbc-yugabytedb-42.7.3-yb-2.jar \
  --driver-class-path /opt/jdbc/jdbc-yugabytedb-42.7.3-yb-2.jar \
  --conf spark.executor.extraClassPath=/opt/jdbc/jdbc-yugabytedb-42.7.3-yb-2.jar \
  ./target/yb-spark-copy-job-1.0.0.jar \
  /src/main/resources/job.properties \
  /sql/entityres_card_merchants_sandbox.sql \
  > /data/yb_gps/cdm/msc/migration/ua_migration_logs/gp_entityres_card_merchants_sandbox_RUN1_migrate_log_scala_version_$(date +%Y%m%d_%H%M%S).txt 2>&1 &
```

Adjust the driver jar path and log path for your environment.

---

## job.properties fixes (full refresh)

### 1) Disable batching for full refresh

Batching by date breaks full refresh aggregates. Use:

```
batch.enabled=false
```

### 2) Use clean JDBC URLs

Do **not** embed user/password in the URL if you already set them as properties.

**Correct format:**

```
source.jdbc.url=jdbc:yugabytedb://<host>:5433/<db>
source.jdbc.user=<user>
source.jdbc.password=<password>

target.jdbc.url=jdbc:yugabytedb://<host>:5433/<db>
target.jdbc.user=<user>
target.jdbc.password=<password>
```

### 3) Full refresh date range

Use a wide range to cover all data:

```
sql.start_date=1900-01-01
sql.end_date=2100-01-01
```

Keep JDBC partition bounds within that range.
