package com.yb.migration.yb

import com.yugabyte.copy.CopyManager
import com.yugabyte.core.BaseConnection
import java.nio.charset.StandardCharsets
import java.sql.Connection

class CopyWriter(
  conn: Connection,
  copySql: String,
  flushEvery: Int
) {
  private val baseConn: BaseConnection = {
    try {
      conn.unwrap(classOf[BaseConnection])
    } catch {
      case _: java.sql.SQLException =>
        conn.asInstanceOf[BaseConnection]
    }
  }

  private val copyManager = new CopyManager(baseConn)
  private val buffer = new StringBuilder(4 * 1024 * 1024)
  private var rowCount = 0L
  private var totalRowsWritten = 0L
  private var copyIn: Option[com.yugabyte.copy.CopyIn] = None

  def start(): Unit = {
    copyIn = Some(copyManager.copyIn(copySql))
  }

  def writeRow(csvRow: String): Unit = {
    buffer.append(csvRow).append('\n')
    rowCount += 1
    if (rowCount >= flushEvery) {
      flush()
    }
  }

  def flush(): Unit = {
    if (buffer.nonEmpty && copyIn.isDefined) {
      val csvData = buffer.toString()
      val bytes = csvData.getBytes(StandardCharsets.UTF_8)
      copyIn.get.writeToCopy(bytes, 0, bytes.length)
      totalRowsWritten += rowCount
      buffer.clear()
      rowCount = 0
    }
  }

  def endCopy(): Long = {
    if (buffer.nonEmpty) {
      flush()
    }
    copyIn match {
      case Some(copy) =>
        val rowsCopied = copy.endCopy()
        copyIn = None
        rowsCopied
      case None =>
        0L
    }
  }

  def cancelCopy(): Unit = {
    copyIn.foreach(_.cancelCopy())
    copyIn = None
    buffer.clear()
    rowCount = 0
  }
}
