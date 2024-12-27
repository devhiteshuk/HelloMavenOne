package spark

import org.apache.spark.{SparkConf, SparkContext}

object BitcoinDataProcessing {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("someName").setAppName("WordCount")
    //create spark context object
    val sc = new SparkContext(conf)


    //Create RDD from parallelize
    val dataSeq = Seq(("Java", 20000), ("Python", 100000), ("Scala", 3000))
    val rdd = sc.parallelize(dataSeq)
    println(rdd.collect().foreach(print))

    val filePath = "\"D:\\Workspace\\BitcoinProject\\assets\\bitcoin_data.csv\""
    println(s"File Path is $filePath")
  }
}
