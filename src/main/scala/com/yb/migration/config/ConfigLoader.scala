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
      sourceHost = getOrDefault("yugabyte.source.host", "localhost"),
      sourceHosts = getOrDefault("yugabyte.source.hosts", "")
        .split(",")
        .map(_.trim)
        .filter(_.nonEmpty)
        .toList,
      sourcePort = getOrDefault("yugabyte.source.port", "5433").toInt,
      sourceDatabase = get("yugabyte.source.database"),
      sourceUser = get("yugabyte.source.username"),
      sourcePassword = get("yugabyte.source.password"),
      sourceJdbcParams = getOrDefault("yugabyte.source.jdbc.params", ""),
      sourceLoadBalanceHosts = getOrDefault("yugabyte.source.loadBalanceHosts", "true").toBoolean,
      targetHost = getOrDefault("yugabyte.target.host", "localhost"),
      targetHosts = getOrDefault("yugabyte.target.hosts", "")
        .split(",")
        .map(_.trim)
        .filter(_.nonEmpty)
        .toList,
      targetPort = getOrDefault("yugabyte.target.port", "5433").toInt,
      targetDatabase = get("yugabyte.target.database"),
      targetUser = get("yugabyte.target.username"),
      targetPassword = get("yugabyte.target.password"),
      targetJdbcParams = getOrDefault("yugabyte.target.jdbc.params", ""),
      targetLoadBalanceHosts = getOrDefault("yugabyte.target.loadBalanceHosts", "true").toBoolean,
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
