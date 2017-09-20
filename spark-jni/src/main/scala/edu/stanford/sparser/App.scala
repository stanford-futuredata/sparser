package edu.stanford.sparser

import org.apache.spark.sql.{DataFrame, SparkSession}

object App {

  val queryMap = Map("zakir1" -> "0",
    "zakir2" -> "1",
    "zakir3" -> "2",
    "zakir4" -> "3",
    "zakir5" -> "4",
    "zakir6" -> "5",
    "zakir7" -> "6",
    "zakir8" -> "7",
    "twitter1" -> "8")

  def queryStrToFilterOp(spark: SparkSession, queryStr: String): (String) => DataFrame = {
    import spark.implicits._
    queryStr match {
      case "0" =>
        (input: String) => {
          spark.read.format("json").load(input).filter($"text".contains("Putin") &&
            $"text".contains("Russia"))
        }
      case "8" =>
        (input: String) => {
          spark.read.format("json").load(input).filter($"text".contains("Donald Trump") &&
            $"created_at".contains("Sep 13"))
        }
    }
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Sparser Spark").getOrCreate()
    val numWorkers: Int = args(0).toInt
    val queryIndexStr: String = queryMap(args(1))
    val jsonFilename: String = args(2)
    val numTrials: Int = args(4).toInt
    val runSparser: Boolean = args(5).equalsIgnoreCase("--sparser")
    val runSpark: Boolean = args(5).equalsIgnoreCase("--spark")
    val runHDFSTest: Boolean = args(5).equalsIgnoreCase("--hdfs")

    val timeParser: () => Unit = {
      if (runSparser) {
        () => {
          val df = spark.read.format("edu.stanford.sparser").options(
            Map("query" -> queryIndexStr)).load(jsonFilename)
          println(df.count())
          println("Num partitions: " + df.rdd.getNumPartitions)
        }
      } else if (runSpark) {
        val filterOp = queryStrToFilterOp(spark, queryIndexStr)
        () => {
          val df = filterOp(jsonFilename)
          println(df.count())
          println("Num partitions: " + df.rdd.getNumPartitions)
        }
      } else if (runHDFSTest) {
        () => {
          val rdd = spark.sparkContext.textFile(jsonFilename)
          println(rdd.count())
          println("Num partitions: " + rdd.getNumPartitions)
        }
      } else {
        throw new RuntimeException(args(5) + " is not a valid argument!")
      }
    }

    val runtimes = new Array[Double](numTrials)
    for (i <- 0 until numTrials) {
      val before = System.currentTimeMillis()
      timeParser()
      val timeMs = System.currentTimeMillis() - before
      println("Total Job Time: " + timeMs / 1000.0)
      runtimes(i) = timeMs
      System.gc()
      spark.sparkContext.parallelize(0 until numWorkers,
        numWorkers).foreach(_ => System.gc())
    }

    println(runtimes.mkString(","))
    val averageRuntime = runtimes.sum[Double] / numTrials
    println("Average Runtime: " + averageRuntime / 1000.0)
    spark.stop()
  }
}
