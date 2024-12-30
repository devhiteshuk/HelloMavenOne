package spark

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object BitcoinDataProcessingNew {

  // Main entry point
  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession.builder
      .appName("Bitcoin Data Processing")
      .master("local[*]") // Adjust the master setting based on your cluster or local setup
      .getOrCreate()

    val filePath = "D:\\Workspace\\BitcoinProject\\assets\\bitcoin_data.csv"
    val processedData = processData(filePath)

    if (processedData != null) {
      processedData.show(5)
      // Save final data to PostgreSQL
      saveToPostgres(processedData)
    }

    spark.stop()
  }

  // Function to load CSV file
  def loadCSV(filePath: String)(implicit spark: SparkSession): DataFrame = {
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(filePath)
  }

  // Function to transform data (filter, add new columns, and other calculations)
  def transformData(df: DataFrame): DataFrame = {
    // Filter out rows where Volume is 0
    val filteredDF = df.filter("Volume > 0")

    // Convert Unix timestamp to human-readable date
    val withDateDF = filteredDF.withColumn("Date", from_unixtime(col("Timestamp")).cast("timestamp"))
    println("==============  withDateDF   ========")
    // Add additional derived columns
    val withPriceChangeDF = withDateDF.withColumn("PriceChange", col("Close") - col("Open"))
    println("==============  withPriceChangeDF   ========")

    // Add daily return percentage
    val withDailyReturnDF = withPriceChangeDF.withColumn("DailyReturn", col("PriceChange") / col("Open") * 100)
    println("==============  withDailyReturnDF   ========")

    // Add Day of Week (e.g., Monday, Tuesday, etc.)
    val withDayOfWeekDF = withDailyReturnDF.withColumn("DayOfWeek", date_format(col("Date"), "EEEE"))
    println("==============  withDayOfWeekDF   ========")

    // Add Cumulative Volume
    val windowSpecCumulative = Window.orderBy("Date")
    val withCumulativeVolumeDF = withDayOfWeekDF.withColumn("CumulativeVolume", sum("Volume").over(windowSpecCumulative))

    // Calculate percentage change in Close price
    val withPercentageChangeDF = withCumulativeVolumeDF.withColumn("PercentageChange", (col("Close") - lag("Close", 1).over(Window.orderBy("Date"))) / lag("Close", 1).over(Window.orderBy("Date")) * 100)
    println("==============  withPercentageChangeDF   ========")

    // Add 5-period Simple Moving Average (SMA) for Close
    val windowSpecSMA = Window.orderBy("Date").rowsBetween(-4, 0)
    val withSMA_DF = withPercentageChangeDF.withColumn("SMA_5", avg("Close").over(windowSpecSMA))

    // Bollinger Bands (Upper and Lower)
    val withBollingerBandsDF = withSMA_DF.withColumn("UpperBand", col("SMA_5") + 2 * stddev("Close").over(windowSpecSMA))
      .withColumn("LowerBand", col("SMA_5") - 2 * stddev("Close").over(windowSpecSMA))

    // Rolling volatility (calculated as standard deviation over 5-period window)
    val withRollingVolatilityDF = withBollingerBandsDF.withColumn("RollingVolatility", stddev("Close").over(windowSpecSMA))

    // Filter extreme values (outliers) based on Z-score (mean = 0, stddev = 1)
    val meanAndStdDev = withRollingVolatilityDF.select(mean("Close").as("mean"), stddev("Close").as("stddev")).head()
    val mean = meanAndStdDev.getAs[Double]("mean")
    val stddev = meanAndStdDev.getAs[Double]("stddev")
    val withZScoreFilteredDF = withRollingVolatilityDF.withColumn("ZScore", (col("Close") - mean) / stddev)
      .filter(abs(col("ZScore")) <= 3) // Remove data points with ZScore > 3 (outliers)

    withZScoreFilteredDF
  }

  // Function to calculate average prices per day
  def calculateAveragePrices(df: DataFrame): DataFrame = {
    df.groupBy("Date")
      .agg(
        avg("Open").alias("avg_open"),
        avg("High").alias("avg_high"),
        avg("Low").alias("avg_low"),
        avg("Close").alias("avg_close")
      )
  }

  // Function to calculate moving average (5-period moving average)
  def calculateMovingAverage(df: DataFrame): DataFrame = {
    val windowSpec = Window.orderBy("Date").rowsBetween(-4, 0)  // 5-period moving average
    df.withColumn("moving_avg_close", avg("Close").over(windowSpec))
  }

  // Main processing function that combines all transformations
  def processData(filePath: String)(implicit spark: SparkSession): DataFrame = {
    val df = loadCSV(filePath)
    val transformedDF = transformData(df)
    transformedDF
  }

  // Function to save processed data to PostgreSQL
  def saveToPostgres(df: DataFrame): Unit = {
    val url = "jdbc:postgresql://18.132.73.146:5432/testdb"
    val properties = new java.util.Properties()
    properties.setProperty("user", "consultants")
    properties.setProperty("password", "WelcomeItc@2022")
    properties.setProperty("driver", "org.postgresql.Driver")
    println("==============  saveToPostgres   ========")
    // Saving data into PostgreSQL as a table
    df.write
      .jdbc(url, "bitcoin_processed_data", properties)
  }
}

