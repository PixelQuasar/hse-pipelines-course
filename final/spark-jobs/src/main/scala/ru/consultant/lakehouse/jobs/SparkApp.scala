package ru.consultant.lakehouse.jobs

import org.apache.spark.sql.SparkSession

object SparkApp {
  def session(appName: String): SparkSession =
    SparkSession.builder().appName(appName).getOrCreate()
}
