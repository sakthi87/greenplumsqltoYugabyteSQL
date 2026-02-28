package com.yb.migration.yb

import com.yb.migration.config.JobConfig

object CopyStatementBuilder {
  def buildCopyStatement(
    config: JobConfig,
    columns: Seq[String]
  ): String = {
    val columnList = columns.mkString(", ")
    val schemaTable = s"${config.targetSchema}.${config.targetTable}"
    val withOptions = scala.collection.mutable.ListBuffer[String]()
    withOptions += "FORMAT csv"
    withOptions += s"DELIMITER '${config.csvDelimiter}'"
    withOptions += s"NULL '${config.csvNull}'"
    withOptions += s"QUOTE '${config.csvQuote}'"
    withOptions += s"ESCAPE '${config.csvEscape}'"
    if (config.copyReplace) {
      withOptions += "REPLACE"
    }
    s"""COPY $schemaTable ($columnList)
       |FROM STDIN
       |WITH (
       |  ${withOptions.mkString(",\n  ")}
       |)""".stripMargin
  }
}
