package edu.stanford.sparser

import org.apache.spark.sql.{DataFrame, SparkSession}

object App {

  val queryMap = Map(
    "zakir1" -> "0",
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
          spark.read.format("json").load(input)//.filter($"p23.telnet.banner.banner" != null &&
            // $"autonomous_system.asn" == 9318)
        }

     case "1" =>
        (input: String) => {
          spark.read.format("json").load(input).filter($"p80.http.get.body".contains("content=\"wordpress 3.51\"")) 
        }

     case "2" =>
        (input: String) => {
          spark.read.format("json").load(input)//.filter($"autonomous_system.asn" == 2516)
        }

     case "3" =>
        (input: String) => {
          spark.read.format("json").load(input)//.filter($"location.country".equals("Chile") &&
            // $"p80.http.get.status_code" != null)
        }

     case "4" =>
        (input: String) => {
          spark.read.format("json").load(input).filter($"p80.http.get.headers.server".contains("DIR-300"))
        }

     case "5" =>
        (input: String) => {
          spark.read.format("json").load(input)//.filter($"p110.pop3.starttls.banner" != null &&
            // $"p995.pop3s.tls.banner" != null)
        }

     case "6" =>
        (input: String) => {
          spark.read.format("json").load(input).filter($"p21.ftp.banner.banner".contains("Seagate Central Shared"))
        }

     case "7" =>
        (input: String) => {
          spark.read.format("json").load(input)//.filter($"p20000.dnp3.status.support" == true)
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
    val numTrials: Int = args(3).toInt
    val runSparser: Boolean = args(4).equalsIgnoreCase("--sparser")
    val runSpark: Boolean = args(4).equalsIgnoreCase("--spark")
    val runHDFSTest: Boolean = args(4).equalsIgnoreCase("--hdfs")

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
