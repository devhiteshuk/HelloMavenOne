package spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

object BitcoinDataProcessing {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("someName").setAppName("WordCount")
    //create spark context object
    val sc = new SparkContext(conf)


    //Create RDD from parallelize
    val dataSeq = Seq(("Java", 20000), ("Python", 100000), ("Scala", 3000))
    val rdd = sc.parallelize(dataSeq)
    println(rdd.collect().foreach(print))

    val filePath = "/tmp/bigdata_nov_2024/hitesh/bitcoin_data.csv"
    println(s"File Path ------> $filePath")
    val spark = SparkSession.builder
      .appName("BitcoinDataProcessing")
      .master("local[*]") // or any other cluster settings
      .getOrCreate()

    val df: DataFrame = spark.read
      .format("com.databricks.spark.csv")  // Use the legacy format
      .option("header", "true")  // Use the first row as headers
      .option("inferSchema", "true")  // Automatically infer data types
      .load(filePath)

    // Show the first few rows of the DataFrame
    df.show()
    println("*************************************************************************************")
    println(s"Data ------> ${df.show(15)}")
    println("*************************************************************************************")
  }
}
