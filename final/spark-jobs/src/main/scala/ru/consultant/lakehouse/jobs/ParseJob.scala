package ru.consultant.lakehouse.jobs

import java.nio.charset.Charset
import java.util.Properties
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import ru.consultant.lakehouse.model.RawEvent
import ru.consultant.lakehouse.parser.SessionParser

object ParseJob {

  // Credentials and endpoints come from environment (single source of truth: .env).
  // Defaults are dev-friendly for running with the bundled compose stack.
  private val ChJdbcUrl      = sys.env.getOrElse("CH_JDBC_URL",  "jdbc:ch://clickhouse:8123/bronze")
  private val ChUser         = sys.env.getOrElse("CH_USER",      "admin")
  private val ChPassword     = sys.env.getOrElse("CH_PASSWORD",  "admin12345")
  private val SessionsPrefix = sys.env.getOrElse("SESSIONS_PREFIX", "s3a://sessions/")
  private val ChDriver       = "com.clickhouse.jdbc.ClickHouseDriver"

  def main(args: Array[String]): Unit = {
    val spark = SparkApp.session("ParseJob")

    // 1. Read all files under s3a://sessions/ as (path, bytes)
    val all = spark.sparkContext.binaryFiles(SessionsPrefix)
    val totalCount = all.count()
    println(s"[ParseJob] discovered $totalCount files")

    if (totalCount == 0) {
      println("[ParseJob] no files to process")
      spark.stop()
      return
    }

    // 2. Parse each file. ClickHouse ReplacingMergeTree deduplicates
    //    (session_id, event_seq) in background, so re-runs over the same
    //    files are idempotent at the storage layer.
    val eventsRdd = all.flatMap { case (path, pds) =>
      val bytes = pds.toArray()
      val content = new String(bytes, Charset.forName("Cp1251"))
      val sessionId = path.split("/").last
      SessionParser.parse(sessionId, content)
    }

    // 3. To DataFrame matching CH schema
    val rows = eventsRdd.map(toRow)
    val df = spark.createDataFrame(rows, BronzeRowSchema)
      .withColumn("event_date", to_date(col("event_time")))
      .select(
        col("session_id"), col("event_seq"), col("event_time"), col("event_date"),
        col("event_type"), col("search_id"), col("search_kind"), col("query_text"),
        col("card_params_json"), col("result_doc_ids_json"), col("doc_id"),
        col("parse_error"), col("raw_line")
      )

    // 4. JDBC append into ClickHouse bronze.events
    val props = new Properties()
    props.put("driver",                   ChDriver)
    props.put("user",                     ChUser)
    props.put("password",                 ChPassword)
    props.put("batchsize",                "20000")
    props.put("rewriteBatchedStatements", "true")

    df.write
      .mode(SaveMode.Append)
      .jdbc(ChJdbcUrl, "events", props)

    val written = df.count()
    println(s"[ParseJob] done; wrote ~$written events to ch.bronze.events")
    spark.stop()
  }

  // ----- schema (matches CH bronze.events DDL) -----

  private val BronzeRowSchema = StructType(Seq(
    StructField("session_id",       StringType,                                  nullable = true),
    StructField("event_seq",        IntegerType,                                 nullable = false),
    StructField("event_time",       TimestampType,                               nullable = true),
    StructField("event_type",       StringType,                                  nullable = true),
    StructField("search_id",        StringType,                                  nullable = true),
    StructField("search_kind",      StringType,                                  nullable = true),
    StructField("query_text",       StringType,                                  nullable = true),
    StructField("card_params_json",    StringType, nullable = true),
    StructField("result_doc_ids_json", StringType, nullable = true),
    StructField("doc_id",           StringType,                                  nullable = true),
    StructField("parse_error",      StringType,                                  nullable = true),
    StructField("raw_line",         StringType,                                  nullable = true)
  ))

  private def toRow(e: RawEvent): Row = Row(
    e.sessionId,
    e.eventSeq,
    e.eventTime.map(i => java.sql.Timestamp.from(i)).orNull,
    e.eventType,
    e.searchId.orNull,
    e.searchKind.orNull,
    e.queryText.orNull,
    cardParamsToJson(e),
    resultDocIdsToJson(e),
    e.docId.orNull,
    e.parseError.orNull,
    e.rawLine.orNull
  )

  /** Serialize Array[String] as compact JSON for CH (Spark JDBC writer doesn't support
    * ArrayType natively; CH side parses with JSONExtractArrayRaw). */
  private def resultDocIdsToJson(e: RawEvent): String = {
    if (e.resultDocIds.isEmpty) "[]"
    else {
      val sb = new StringBuilder("[")
      var first = true
      e.resultDocIds.foreach { id =>
        if (!first) sb.append(',')
        first = false
        sb.append('"').append(escape(id)).append('"')
      }
      sb.append("]")
      sb.toString
    }
  }

  /** Serialize Array<Struct<param_id, value>> as compact JSON for CH (avoids JDBC Tuple quirks). */
  private def cardParamsToJson(e: RawEvent): String = {
    if (e.cardParams.isEmpty) "[]"
    else {
      val sb = new StringBuilder("[")
      var first = true
      e.cardParams.foreach { p =>
        if (!first) sb.append(',')
        first = false
        sb.append("""{"param_id":"""")
          .append(escape(p.paramId))
          .append("""","value":"""")
          .append(escape(p.value))
          .append("\"}")
      }
      sb.append("]")
      sb.toString
    }
  }

  private def escape(s: String): String = {
    val sb = new StringBuilder(s.length)
    s.foreach {
      case '"'  => sb.append("\\\"")
      case '\\' => sb.append("\\\\")
      case '\n' => sb.append("\\n")
      case '\r' => sb.append("\\r")
      case '\t' => sb.append("\\t")
      case c if c < 0x20 => sb.append(f"\\u${c.toInt}%04x")
      case c => sb.append(c)
    }
    sb.toString
  }
}
