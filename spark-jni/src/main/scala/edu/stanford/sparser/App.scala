package edu.stanford.sparser

import org.apache.spark.sql.SparkSession

object App {


  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Sparser Spark").getOrCreate()
    val numWorkers: Int = args(0).toInt
    val queryStr: String = args(1)
    val jsonFilename: String = args(2)
    val numTrials: Int = args(3).toInt
    val runSparser: Boolean = args(4).equalsIgnoreCase("--sparser")
    val runSpark: Boolean = args(4).equalsIgnoreCase("--spark")
    val runReadOnly: Boolean = args(4).equalsIgnoreCase("--read-only")
    val runQueryOnly: Boolean = args(4).equalsIgnoreCase("--query-only")

    val timeJob: () => Unit = {
      if (runSparser) {
        val queryOp = Queries.queryStrToQuery(spark, queryStr)
        () => {
          val df = spark.read.format("edu.stanford.sparser")
            .schema(Queries.queryStrToSchema(queryStr))
            .options(Map("query" -> Queries.sparserQueryMap(queryStr)))
            .load(jsonFilename)
          println("Num rows in query output: " + queryOp(df))
          println("Num partitions: " + df.rdd.getNumPartitions)
        }
      } else if (runSpark) {
        val parserOp = Queries.queryStrToQueryParser(spark, queryStr)
        val queryOp = Queries.queryStrToQuery(spark, queryStr)
        () => {
          val df = parserOp(jsonFilename)
          println("Num rows in query output: " + queryOp(df))
          println("Num partitions: " + df.rdd.getNumPartitions)
        }
      } else if (runQueryOnly) {
        val parserOp = Queries.queryStrToQueryParser(spark, queryStr)
        val queryOp = Queries.queryStrToQuery(spark, queryStr)
        () => {
          val df = parserOp(jsonFilename)
          df.cache()
          val startTime = System.currentTimeMillis()
          println("Num rows in query output: " + queryOp(df))
          val queryTime = System.currentTimeMillis() - startTime
          println("Query Time: " + queryTime / 1000.0)
          println("Num partitions: " + df.rdd.getNumPartitions)
        }
      } else if (runReadOnly) {
        () => {
          val rdd = spark.sparkContext.textFile(jsonFilename)
          println(rdd.count())
          println("Num partitions: " + rdd.getNumPartitions)
        }
      } else {
        throw new RuntimeException(args(4) + " is not a valid argument!")
      }
    }

    val runtimes = new Array[Double](numTrials)
    for (i <- 0 until numTrials) {
      val before = System.currentTimeMillis()
      timeJob()
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
