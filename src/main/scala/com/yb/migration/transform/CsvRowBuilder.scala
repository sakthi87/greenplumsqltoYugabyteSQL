package com.yb.migration.transform

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

object CsvRowBuilder {
  def toCsv(
    row: Row,
    schema: StructType,
    delimiter: String,
    nullToken: String,
    quote: String,
    escape: String
  ): String = {
    val delimChar = if (delimiter.nonEmpty) delimiter.charAt(0) else ','
    val quoteChar = if (quote.nonEmpty) quote.charAt(0) else '"'
    val escapeChar = if (escape.nonEmpty) escape.charAt(0) else '"'

    val builder = new StringBuilder
    var i = 0
    while (i < schema.fields.length) {
      if (i > 0) builder.append(delimChar)
      if (row.isNullAt(i)) {
        builder.append(nullToken)
      } else {
        val value = row.get(i).toString
        val needsQuote =
          value.indexOf(delimChar) >= 0 ||
            value.indexOf('\n') >= 0 ||
            value.indexOf('\r') >= 0 ||
            value.indexOf(quoteChar) >= 0

        if (needsQuote) {
          builder.append(quoteChar)
          var j = 0
          while (j < value.length) {
            val ch = value.charAt(j)
            if (ch == quoteChar) {
              builder.append(escapeChar).append(quoteChar)
            } else {
              builder.append(ch)
            }
            j += 1
          }
          builder.append(quoteChar)
        } else {
          builder.append(value)
        }
      }
      i += 1
    }
    builder.toString()
  }
}
