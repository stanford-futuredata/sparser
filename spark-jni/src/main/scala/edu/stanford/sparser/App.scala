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
    val runSparser: Boolean = args(5).equalsIgnoreCase("--sparser")
    val runSpark: Boolean = args(5).equalsIgnoreCase("--spark")
    val runHDFSTest: Boolean = args(5).equalsIgnoreCase("--hdfs")

    val timeParser: () => Unit = {
      if (runSparser) {
        () => {
          val df = spark.read.format("edu.stanford.sparser").options(
            Map("filters" -> filterStrs, "projections" -> projectionStrs)).load(jsonFilename)
          println(df.count())
          println("Num partitions: " + df.rdd.getNumPartitions)
        }
      } else if (runSpark) {
	() => {
          val df = spark.read.format("json").load(jsonFilename)
          println(df.filter($"text".contains("Putin") &&
	    $"text".contains("Russia")).count())
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
