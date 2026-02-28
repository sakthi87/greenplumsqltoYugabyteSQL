package com.yb.migration

import com.yb.migration.config.{ConfigLoader, JobConfig}
import com.yb.migration.transform.CsvRowBuilder
import com.yb.migration.yb.{CopyStatementBuilder, CopyWriter}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.nio.file.{Files, Paths}
import java.sql.{DriverManager, Date => SqlDate}
import java.util.Properties

object QueryCopyJob {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Usage: QueryCopyJob <job.properties> <sql_file>")
      System.exit(1)
    }

    val configPath = args(0)
    val sqlPath = args(1)
    val config = ConfigLoader.load(configPath)
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
    val dbTable = s"($sql) AS src"

    val baseReader = spark.read
    val usePartitioning =
      config.jdbcPartitionColumn.nonEmpty &&
        config.jdbcPartitionLower.nonEmpty &&
        config.jdbcPartitionUpper.nonEmpty &&
        config.jdbcPartitionNum > 1

    if (usePartitioning) {
      baseReader.jdbc(
        config.sourceJdbcUrl,
        dbTable,
        config.jdbcPartitionColumn,
        config.jdbcPartitionLower,
        config.jdbcPartitionUpper,
        config.jdbcPartitionNum,
        props
      )
    } else {
      baseReader.jdbc(config.sourceJdbcUrl, dbTable, props)
    }
  }

  private def writeWithCopy(df: DataFrame, config: JobConfig): Unit = {
    val columns = df.schema.fields.map(_.name).toSeq
    val copySql = CopyStatementBuilder.buildCopyStatement(config, columns)
    val schema = df.schema

    df.foreachPartition { rows: Iterator[Row] =>
      if (rows.nonEmpty) {
        val conn = DriverManager.getConnection(
          config.targetJdbcUrl,
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
}
