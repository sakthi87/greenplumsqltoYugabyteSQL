package com.yb.migration.config

import java.io.FileInputStream
import java.util.Properties

object ConfigLoader {
  def load(path: String): JobConfig = {
    val props = new Properties()
    val stream = new FileInputStream(path)
    try {
      props.load(stream)
    } finally {
      stream.close()
    }

    def get(key: String): String = {
      val value = props.getProperty(key)
      if (value == null || value.trim.isEmpty) {
        throw new IllegalArgumentException(s"Missing required property: $key")
      }
      value.trim
    }

    def getOrDefault(key: String, defaultValue: String): String = {
      val value = props.getProperty(key)
      if (value == null || value.trim.isEmpty) defaultValue else value.trim
    }

    JobConfig(
      sourceJdbcUrl = get("yugabyte.source.jdbc.url"),
      sourceUser = get("yugabyte.source.username"),
      sourcePassword = get("yugabyte.source.password"),
      sourceJdbcHosts = getOrDefault("yugabyte.source.hosts", "")
        .split(",")
        .map(_.trim)
        .filter(_.nonEmpty)
        .toList,
      sourceJdbcPort = getOrDefault("yugabyte.source.port", "5433").toInt,
      sourceJdbcDatabase = getOrDefault("yugabyte.source.database", ""),
      sourceJdbcParams = getOrDefault("yugabyte.source.jdbc.params", ""),
      sourceJdbcLoadBalance = getOrDefault("yugabyte.source.loadBalanceHosts", "true").toBoolean,
      targetJdbcUrl = get("yugabyte.target.jdbc.url"),
      targetUser = get("yugabyte.target.username"),
      targetPassword = get("yugabyte.target.password"),
      targetJdbcHosts = getOrDefault("yugabyte.target.hosts", "")
        .split(",")
        .map(_.trim)
        .filter(_.nonEmpty)
        .toList,
      targetJdbcPort = getOrDefault("yugabyte.target.port", "5433").toInt,
      targetJdbcDatabase = getOrDefault("yugabyte.target.database", ""),
      targetJdbcParams = getOrDefault("yugabyte.target.jdbc.params", ""),
      targetJdbcLoadBalance = getOrDefault("yugabyte.target.loadBalanceHosts", "true").toBoolean,
      targetSchema = get("target.schema"),
      targetTable = get("target.table"),
      sqlStartDate = getOrDefault("sql.start_date", "1970-01-01"),
      sqlEndDate = getOrDefault("sql.end_date", "2100-01-01"),
      batchEnabled = getOrDefault("batch.enabled", "false").toBoolean,
      batchWindowDays = getOrDefault("batch.window_days", "1").toInt,
      batchMaxBatches = getOrDefault("batch.max_batches", "0").toInt,
      copyReplace = getOrDefault("copy.replace", "true").toBoolean,
      csvDelimiter = getOrDefault("csv.delimiter", ","),
      csvNull = getOrDefault("csv.null", "\\N"),
      csvQuote = getOrDefault("csv.quote", "\""),
      csvEscape = getOrDefault("csv.escape", "\""),
      copyFlushEvery = getOrDefault("copy.flush_every", "20000").toInt,
      fetchSize = getOrDefault("jdbc.fetch_size", "10000").toInt,
      repartition = getOrDefault("spark.repartition", "0").toInt,
      disableTransactionalWrites = getOrDefault("yb.disable_transactional_writes", "true").toBoolean,
      jdbcPartitionColumn = getOrDefault("jdbc.partition.column", ""),
      jdbcPartitionLower = getOrDefault("jdbc.partition.lower", ""),
      jdbcPartitionUpper = getOrDefault("jdbc.partition.upper", ""),
      jdbcPartitionNum = getOrDefault("jdbc.partition.num", "0").toInt
    )
  }
}
