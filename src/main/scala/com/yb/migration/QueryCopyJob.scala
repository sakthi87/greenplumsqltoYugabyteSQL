package com.yb.migration

import com.yb.migration.config.{ConfigLoader, JobConfig}
import com.yb.migration.transform.CsvRowBuilder
import com.yb.migration.yb.{CopyStatementBuilder, CopyWriter}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.nio.file.{Files, Paths}
import java.sql.{DriverManager, Date => SqlDate}
import java.util.Properties
import java.time.LocalDate

object QueryCopyJob {
  private val JdbcDriverClass = "com.yugabyte.Driver"

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Usage: QueryCopyJob <job.properties> <sql_file>")
      System.exit(1)
    }

    val configPath = args(0)
    val sqlPath = args(1)
    val config = ConfigLoader.load(configPath)
    // Ensure JDBC driver is loaded and registered for Spark JDBC reader
    Class.forName(JdbcDriverClass)
    val rawSql = new String(Files.readAllBytes(Paths.get(sqlPath)), "UTF-8")

    val spark = SparkSession.builder().appName("YB Query COPY Job").getOrCreate()
    try {
      runBatches(spark, config, rawSql)
    } finally {
      spark.stop()
    }
  }

  private def renderSql(rawSql: String, startDate: String, endDate: String): String = {
    rawSql
      .replace("{{start_date}}", startDate)
      .replace("{{end_date}}", endDate)
      .trim
      .stripSuffix(";")
  }

  private def runBatches(spark: SparkSession, config: JobConfig, rawSql: String): Unit = {
    if (!config.batchEnabled) {
      val sql = renderSql(rawSql, config.sqlStartDate, config.sqlEndDate)
      val df = readQuery(spark, config, sql)
      val finalDf = if (config.repartition > 0) df.repartition(config.repartition) else df
      writeWithCopy(finalDf, config)
      return
    }

    val start = SqlDate.valueOf(config.sqlStartDate)
    val end = SqlDate.valueOf(config.sqlEndDate)
    val windowDays = math.max(config.batchWindowDays, 1)
    val maxBatches = config.batchMaxBatches

    var batchStart = start.toLocalDate
    var batchesRun = 0
    while (!batchStart.isAfter(end.toLocalDate.minusDays(1)) && (maxBatches <= 0 || batchesRun < maxBatches)) {
      val batchEnd = batchStart.plusDays(windowDays.toLong)
      val sql = renderSql(rawSql, batchStart.toString, batchEnd.toString)
      val df = readQuery(spark, config, sql)
      val finalDf = if (config.repartition > 0) df.repartition(config.repartition) else df
      writeWithCopy(finalDf, config)
      batchStart = batchEnd
      batchesRun += 1
    }
  }

  private def readQuery(spark: SparkSession, config: JobConfig, sql: String): DataFrame = {
    val props = new Properties()
    props.put("user", config.sourceUser)
    props.put("password", config.sourcePassword)
    props.put("fetchsize", config.fetchSize.toString)
    props.put("driver", JdbcDriverClass)
    // Read-optimized JDBC properties (safe defaults)
    props.put("preferQueryMode", "simple")
    props.put("binaryTransfer", "false")
    props.put("stringtype", "unspecified")
    props.put("reWriteBatchedInserts", "true")
    props.put("connectTimeout", "10")
    props.put("loginTimeout", "10")
    props.put("socketTimeout", "0")
    props.put("tcpKeepAlive", "true")
    props.put("keepAlive", "true")
    val dbTable = s"($sql) AS src"
    val sourceJdbcUrl = buildSourceJdbcUrl(config)

    val baseReader = spark.read
    val usePartitioning =
      config.jdbcPartitionColumn.nonEmpty &&
        config.jdbcPartitionLower.nonEmpty &&
        config.jdbcPartitionUpper.nonEmpty &&
        config.jdbcPartitionNum > 1

    if (!usePartitioning) {
      return baseReader.jdbc(sourceJdbcUrl, dbTable, props)
    }

    val lower = config.jdbcPartitionLower
    val upper = config.jdbcPartitionUpper
    val num = config.jdbcPartitionNum

    def isNumeric(value: String): Boolean = {
      if (value == null || value.isEmpty) return false
      val start = if (value.charAt(0) == '-') 1 else 0
      if (start == value.length) return false
      value.substring(start).forall(_.isDigit)
    }

    if (isNumeric(lower) && isNumeric(upper)) {
      baseReader.jdbc(
        sourceJdbcUrl,
        dbTable,
        config.jdbcPartitionColumn,
        lower.toLong,
        upper.toLong,
        num,
        props
      )
    } else {
      val startDate = LocalDate.parse(lower)
      val endDate = LocalDate.parse(upper)
      val totalDays = Math.max(1L, java.time.temporal.ChronoUnit.DAYS.between(startDate, endDate))
      val step = Math.max(1L, Math.ceil(totalDays.toDouble / num.toDouble).toLong)
      val predicates = scala.collection.mutable.ArrayBuffer[String]()
      var current = startDate
      while (current.isBefore(endDate)) {
        val next = current.plusDays(step)
        val end = if (next.isAfter(endDate)) endDate else next
        val predicate =
          s"${config.jdbcPartitionColumn} >= DATE '${current.toString}' AND ${config.jdbcPartitionColumn} < DATE '${end.toString}'"
        predicates += predicate
        current = end
      }
      baseReader.jdbc(sourceJdbcUrl, dbTable, predicates.toArray, props)
    }
  }

  private def buildSourceJdbcUrl(config: JobConfig): String = {
    val hosts = if (config.sourceHosts.nonEmpty) config.sourceHosts else List(config.sourceHost)
    val hostList = hosts.mkString(",")
    val base = s"jdbc:yugabytedb://$hostList:${config.sourcePort}/${config.sourceDatabase}"
    val params = buildSourceJdbcParams(config)
    if (params.nonEmpty) s"$base?$params" else base
  }

  private def buildSourceJdbcParams(config: JobConfig): String = {
    val params = scala.collection.mutable.ListBuffer[String]()
    if (config.sourceLoadBalanceHosts) {
      params += "load-balance=true"
    }
    if (config.sourceJdbcParams.nonEmpty) {
      params += config.sourceJdbcParams
    }
    params.mkString("&")
  }

  private def writeWithCopy(df: DataFrame, config: JobConfig): Unit = {
    val columns = df.schema.fields.map(_.name).toSeq
    val copySql = CopyStatementBuilder.buildCopyStatement(config, columns)
    val schema = df.schema
    val targetJdbcUrl = buildTargetJdbcUrl(config)

    df.foreachPartition { rows: Iterator[Row] =>
      if (rows.nonEmpty) {
        val conn = DriverManager.getConnection(
          targetJdbcUrl,
          config.targetUser,
          config.targetPassword
        )
        conn.setAutoCommit(false)
        if (config.disableTransactionalWrites) {
          val stmt = conn.createStatement()
          try {
            stmt.execute("SET yb_disable_transactional_writes = on;")
          } finally {
            stmt.close()
          }
        }

        val writer = new CopyWriter(conn, copySql, config.copyFlushEvery)
        try {
          writer.start()
          rows.foreach { row =>
            val csvRow = CsvRowBuilder.toCsv(
              row,
              schema,
              config.csvDelimiter,
              config.csvNull,
              config.csvQuote,
              config.csvEscape
            )
            writer.writeRow(csvRow)
          }
          writer.endCopy()
          conn.commit()
        } catch {
          case e: Exception =>
            try {
              conn.rollback()
            } catch {
              case _: Exception => // ignore rollback failures
            }
            writer.cancelCopy()
            throw e
        } finally {
          try {
            conn.close()
          } catch {
            case _: Exception => // ignore close failures
          }
        }
      }
    }
  }

  private def buildTargetJdbcUrl(config: JobConfig): String = {
    val hosts = if (config.targetHosts.nonEmpty) config.targetHosts else List(config.targetHost)
    val hostList = hosts.mkString(",")
    val base = s"jdbc:yugabytedb://$hostList:${config.targetPort}/${config.targetDatabase}"
    val params = buildTargetJdbcParams(config)
    if (params.nonEmpty) s"$base?$params" else base
  }

  private def buildTargetJdbcParams(config: JobConfig): String = {
    val params = scala.collection.mutable.ListBuffer[String]()
    if (config.targetLoadBalanceHosts) {
      params += "load-balance=true"
    }
    if (config.targetJdbcParams.nonEmpty) {
      params += config.targetJdbcParams
    }
    params.mkString("&")
  }
}
