package ru.consultant.lakehouse.jobs

import java.nio.charset.Charset
import java.util.Properties
import scala.collection.JavaConverters._
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import ru.consultant.lakehouse.model.RawEvent
import ru.consultant.lakehouse.parser.SessionParser

object ParseJob {

  private val ChJdbcUrl      = sys.env.getOrElse("CH_JDBC_URL",  "jdbc:ch://clickhouse:8123/bronze")
  private val ChUser         = sys.env.getOrElse("CH_USER",      "admin")
  private val ChPassword     = sys.env.getOrElse("CH_PASSWORD",  "admin12345")
  private val SessionsPrefix = sys.env.getOrElse("SESSIONS_PREFIX", "s3a://sessions/")
  private val ChDriver       = "com.clickhouse.jdbc.ClickHouseDriver"

  def main(args: Array[String]): Unit = {
    val spark = SparkApp.session("ParseJob")

    val jdbcProps = new Properties()
    jdbcProps.put("driver",                   ChDriver)
    jdbcProps.put("user",                     ChUser)
    jdbcProps.put("password",                 ChPassword)
    jdbcProps.put("batchsize",                "20000")
    jdbcProps.put("rewriteBatchedStatements", "true")

    val processedDF = spark.read.jdbc(
      ChJdbcUrl,
      "(SELECT DISTINCT path FROM bronze.processed_files) AS p",
      jdbcProps
    )
    val processedSet = processedDF.collect().map(_.getString(0)).toSet
    val processedBC  = spark.sparkContext.broadcast(processedSet)
    println(s"[ParseJob] watermark: ${processedSet.size} files already ingested")

    val all      = spark.sparkContext.binaryFiles(SessionsPrefix)
    val newRddRaw = all.filter { case (p, _) => !processedBC.value.contains(p) }
    val MaxFilesPerRun = sys.env.getOrElse("MAX_FILES_PER_RUN", "5000").toInt
    val newRdd = newRddRaw.zipWithIndex().filter(_._2 < MaxFilesPerRun).map(_._1).cache()
    val newCount = newRdd.count()
    println(s"[ParseJob] processing $newCount new files this run (cap = $MaxFilesPerRun)")

    if (newCount == 0) {
      println("[ParseJob] no new files; exiting")
      newRdd.unpersist()
      spark.stop()
      return
    }

    val eventsRdd = newRdd.flatMap { case (path, pds) =>
      val bytes     = pds.toArray()
      val content   = new String(bytes, Charset.forName("Cp1251"))
      val sessionId = path.split("/").last
      SessionParser.parse(sessionId, content)
    }

    val rows = eventsRdd.map(toRow)
    val df = spark.createDataFrame(rows, BronzeRowSchema)
      .withColumn("event_date", to_date(col("event_time")))
      .select(
        col("session_id"), col("event_seq"), col("event_time"), col("event_date"),
        col("event_type"), col("search_id"), col("search_kind"), col("query_text"),
        col("card_params_json"), col("result_doc_ids_json"), col("doc_id"),
        col("parse_error"), col("raw_line")
      )

    df.write
      .mode(SaveMode.Append)
      .jdbc(ChJdbcUrl, "events", jdbcProps)
    println(s"[ParseJob] wrote events from ${newCount} new files into bronze.events")

    val newPaths = newRdd.keys.collect()
    val pathsDF = spark.createDataFrame(
      newPaths.toSeq.map(Row(_)).asJava,
      StructType(Seq(StructField("path", StringType, nullable = false)))
    )
    pathsDF.write
      .mode(SaveMode.Append)
      .jdbc(ChJdbcUrl, "processed_files", jdbcProps)
    println(s"[ParseJob] watermark advanced by ${newPaths.length} entries")

    newRdd.unpersist()
    spark.stop()
  }

  private val BronzeRowSchema = StructType(Seq(
    StructField("session_id",          StringType,    nullable = true),
    StructField("event_seq",           IntegerType,   nullable = false),
    StructField("event_time",          TimestampType, nullable = true),
    StructField("event_type",          StringType,    nullable = true),
    StructField("search_id",           StringType,    nullable = true),
    StructField("search_kind",         StringType,    nullable = true),
    StructField("query_text",          StringType,    nullable = true),
    StructField("card_params_json",    StringType,    nullable = true),
    StructField("result_doc_ids_json", StringType,    nullable = true),
    StructField("doc_id",              StringType,    nullable = true),
    StructField("parse_error",         StringType,    nullable = true),
    StructField("raw_line",            StringType,    nullable = true)
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
