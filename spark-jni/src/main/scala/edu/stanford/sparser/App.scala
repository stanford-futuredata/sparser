package edu.stanford.sparser

import org.apache.spark.sql.SparkSession

object App {


  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Sparser Spark").getOrCreate()
    val numWorkers: Int = args(0).toInt
    val queryStr: String = args(1)
    val filename: String = args(2)
    val numTrials: Int = args(3).toInt


    if (filename.endsWith(".json")) {
      spark.sqlContext.setConf("spark.sql.sources.default", "json")
    } else if (filename.endsWith(".parquet")) {
      spark.sqlContext.setConf("spark.sql.sources.default", "parquet")
    } else {
      throw new RuntimeException(s"$filename has file format that is not supported")
    }

    val timeJob: () => Unit = {
      args(4).toLowerCase match {
        case "--sparser" =>
          if (filename.endsWith(".parquet")) {
            throw new RuntimeException("can't run Sparser on Parquet files yet")
          }
          val queryOp = Queries.queryStrToQuery(spark, queryStr)
          () => {
            val df = spark.read.format("edu.stanford.sparser")
              .schema(Queries.queryStrToSchema(queryStr))
              .options(Map("query" -> Queries.sparserQueryMap(queryStr)))
              .load(filename)
            println("Num rows in query output: " + queryOp(df))
            println("Num partitions: " + df.rdd.getNumPartitions)
          }
        case "--spark" =>
          val parserOp = Queries.queryStrToQueryParser(spark, queryStr)
          val queryOp = Queries.queryStrToQuery(spark, queryStr)
          () => {
            val df = parserOp(filename)
            println("Num rows in query output: " + queryOp(df))
            println("Num partitions: " + df.rdd.getNumPartitions)
          }
        case "--read-only" =>
          () => {
            val rdd = spark.sparkContext.textFile(filename)
            println(rdd.count())
            println("Num partitions: " + rdd.getNumPartitions)
          }
        case "--query-only" =>
          val parserOp = Queries.queryStrToQueryParser(spark, queryStr)
          val queryOp = Queries.queryStrToQuery(spark, queryStr)
          () => {
            val df = parserOp(filename)
            df.cache()
            val startTime = System.currentTimeMillis()
            println("Num rows in query output: " + queryOp(df))
            val queryTime = System.currentTimeMillis() - startTime
            println("Query Time: " + queryTime / 1000.0)
            println("Num partitions: " + df.rdd.getNumPartitions)
          }
        case _ =>
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
