package edu.stanford.sparser

import org.apache.spark.sql.SparkSession

object App {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Sparser Spark").getOrCreate()
    import spark.implicits._
    val numWorkers: Int = args(0).toInt
    val filterStrs: String = args(1)
    val projectionStrs: String = args(2)
    val jsonFilename: String = args(3) // File in HDFS
    val numTrials: Int = args(4).toInt
    val runSparser: Boolean = args.length >= 6 && args(5).equalsIgnoreCase("--sparser")

    val timeParser: () => Unit = {
      if (runSparser) {
        () => {
          val df = spark.read.format("edu.stanford.sparser").options(
            Map("filters" -> filterStrs, "projections" -> projectionStrs)).load(jsonFilename)
          println(df.count())
        }
      } else {
        () => {
          val df = spark.read.format("json").load(jsonFilename)
          println(df.filter($"text".contains("Putin") &&
            $"text".contains("Russia")).count())
        }
      }
    }

    val runtimes = new Array[Double](numTrials)
    for (i <- 0 until numTrials) {
      val before = System.currentTimeMillis()
      timeParser()
      val timeMs = System.currentTimeMillis() - before
      println(timeMs / 1000.0)
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
