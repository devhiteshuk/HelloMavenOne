package spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, col, from_unixtime}

object BitcoinDataProcessing {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("someName").setAppName("WordCount")
    //create spark context object
    val sc = new SparkContext(conf)

    val filePath = "/tmp/bigdata_nov_2024/hitesh/bitcoin_data.csv"
    println(s"File Path ------> $filePath")
    val spark = SparkSession.builder
      .appName("BitcoinDataProcessing")
      .master("local[*]") // or any other cluster settings
      .getOrCreate()

    val df: DataFrame = spark.read
      .format("com.databricks.spark.csv") // Use the legacy format
      .option("header", "true") // Use the first row as headers
      .option("inferSchema", "true") // Automatically infer data types
      .load(filePath)

    // Show the first few rows of the DataFrame
    df.show()
    println("*************************************************************************************")
    println(s"Data ------> ${df.show(15)}")
    println("*************************************************************************************")

    transformData(df)
    //calculateAveragePrices(df)
    //calculateMovingAverage(df)
    spark.stop()

  }


  // Function to transform data (filter and add new columns)
  def transformData(df: DataFrame): DataFrame = {
    df.filter("Volume > 0") // Filter out rows with Volume = 0
      .withColumn("Date", from_unixtime(col("Timestamp")).cast("timestamp")) // Convert Timestamp to Date

    print(s"New Field added ${df.show(5)}")

    df
  }

  // Function to calculate average prices per day
  def calculateAveragePrices(df: DataFrame): DataFrame = {
    println("called: calculateAveragePrices")
    df.groupBy("Date")
      .agg(
        avg("Open").alias("avg_open"),
        avg("High").alias("avg_high"),
        avg("Low").alias("avg_low"),
        avg("Close").alias("avg_close")
      )
  }

  // Function to calculate a moving average (5-period moving average example)
  def calculateMovingAverage(df: DataFrame): DataFrame = {
    println("called: calculateMovingAverage")
    val windowSpec = Window.orderBy("Date").rowsBetween(-4, 0) // 5-period moving average
    df.withColumn("moving_avg_close", avg("Close").over(windowSpec))
  }

}
