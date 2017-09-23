package edu.stanford.sparser

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object Queries {

  /**
    * Just parsing, filters, and projections
    */
  def queryStrToQueryParser(spark: SparkSession, queryStr: String): (Dataset[String]) => DataFrame = {
    import spark.implicits._

    queryStr match {
      /************* Zakir Queries *************/
      case "0" =>
         /**
          * SELECT COUNT(*)
          * FROM  ipv4.20160425
          * WHERE p23.telnet.banner.banner is not NULL
          * AND   autonomous_system.asn = 9318;
          **/
        (input: Dataset[String]) => {
          spark.read.json(input).filter($"autonomous_system.asn" === 9318).filter(
            "p23.telnet.banner.banner is not null")
        }

      case "1" =>
         /**
          * SELECT COUNT(*)
          * FROM  ipv4.20160425
          * WHERE p80.http.get.body CONTAINS 'content=\"WordPress 4.0';
          **/
        (input: Dataset[String]) => {
          spark.read.json(input).filter($"p80.http.get.body".contains("content=\\\"WordPress 4.0"))
        }

      case "2" =>
         /**
          * SELECT COUNT(*)
          * FROM  ipv4.20160425
          * WHERE autonomous_system.asn = 2516;
          **/
        (input: Dataset[String]) => {
          spark.read.json(input).filter($"autonomous_system.asn" === 2516)
        }

      case "3" =>
         /**
          * SELECT COUNT(*)
          * FROM  ipv4.20160425
          * WHERE location.country = "Chile"
          * AND   p80.http.get.status_code is not NULL;
          **/
        (input: Dataset[String]) => {
          spark.read.json(input).filter($"location.country" === "Chile").filter(
            "p80.http.get.status_code is not null")
        }

      case "4" =>
         /**
          * SELECT COUNT(*)
          * FROM ipv4.20160425
          * WHERE p80.http.get.headers.server like '%DIR-300%';
          **/
        (input: Dataset[String]) => {
          spark.read.json(input).filter($"p80.http.get.headers.server".contains("DIR-300"))
        }

      case "5" =>
         /**
          * SELECT COUNT(*)
          * FROM ipv4.20160425
          * WHERE p110.pop3s.starttls.banner is not NULL;
          **/
        (input: Dataset[String]) => {
          spark.read.json(input).filter("p110.pop3.starttls.banner is not null")
        }

      case "6" =>
         /**
          * SELECT COUNT(*)
          * FROM ipv4.20160425
          * WHERE p21.ftp.banner.banner like '%Seagate Central Shared%';
          **/
        (input: Dataset[String]) => {
          spark.read.json(input).filter($"p21.ftp.banner.banner".contains("Seagate Central Shared"))
        }

      case "7" =>
         /**
          * SELECT COUNT(*)
          * FROM ipv4.20160425
          * WHERE p20000.dnp3.status.support = true;
          **/
        (input: Dataset[String]) => {
          spark.read.json(input).filter($"p20000.dnp3.status.support" === true)
        }

      case "8" =>
         /**
          * SELECT autonomous_system.asn, count(ipint) AS count
          * FROM ipv4.20160425
          * WHERE autonomous_system.name CONTAINS 'Verizon'
          * GROUP BY autonomous_system.asn;
          **/
        (input: Dataset[String]) => {
          spark.read.json(input).filter($"autonomous_system.name".contains("Verizon"))
            .select($"autonomous_system.asn", $"ipint")
        }

      case "9" =>
         /**
          * SELECT autonomous_system.asn AS asn, COUNT(ipint) AS hosts
          * FROM ipv4.20160425
          * WHERE p502.modbus.device_id.function_code is not NULL
          * GROUP BY asn ORDER BY asn DESC;
          **/
        (input: Dataset[String]) => {
          spark.read.json(input).filter($"p502.modbus.device_id.function_code is not NULL")
            .select($"autonomous_system.asn", $"ipint")
        }

      /************* Twitter Queries *************/
      case "10" =>
         /**
          * SELECT count(*)
          * FROM tweets
          * WHERE text contains "Donald Trump"
          * AND created_at contains "Sep 13";
          **/
        (input: Dataset[String]) => {
          spark.read.json(input).filter($"text".contains("Donald Trump") &&
            $"created_at".contains("Sep 13"))
        }

      case "11" =>
         /**
          * SELECT user.id, SUM(retweet_count)
          * FROM tweets
          * WHERE text contains "Obama"
          *  GROUP BY user.id;
          **/
        (input: Dataset[String]) => {
          spark.read.json(input).filter($"text".contains("Obama"))
            .select($"user.id", $"retweet_count")
        }

      case "12" =>
         /**
          * SELECT id
          * FROM tweets
          * WHERE user.lang = "msa";
          **/
        (input: Dataset[String]) => {
          spark.read.json(input).filter($"user.lang" === "msa").select($"id")
        }

      case "13" =>
         /**
          * SELECT distinct user.id
          * FROM tweets
          * WHERE text contains @realDonaldTrump;
          **/
        (input: Dataset[String]) => {
          spark.read.json(input).filter($"text".contains("@realDonaldTrump"))
            .select($"user.id")
        }
    }
  }

  /**
    * The query itself: could be a COUNT, GROUP BY, DISTINCT, ORDER BY, etc. Return
    * the number of rows of the final dataframe (i.e., call count())
    */
  def queryStrToQuery(spark: SparkSession, queryStr: String): (DataFrame) => Long = {
    import spark.implicits._
    queryStr match {
      case "8" =>
        /**
          * SELECT autonomous_system.asn, count(ipint) AS count
          * FROM ipv4.20160425
          * WHERE autonomous_system.name CONTAINS 'Verizon'
          * GROUP BY autonomous_system.asn;
          */
        (df: DataFrame) => {
          df.groupBy($"autonomous_system.asn").count().count()
        }
      case "9" =>
         /**
          * SELECT autonomous_system.asn AS asn, COUNT(ipint) AS hosts
          * FROM ipv4.20160425
          * WHERE p502.modbus.device_id.function_code is not NULL
          * GROUP BY asn ORDER BY asn DESC;
          */
        (df: DataFrame) => {
          df.groupBy("autonomous_system.asn").count()
            .orderBy($"autonomous_system.asn".desc).count()
        }
      case "11" =>
        /**
          * SELECT user.id, SUM(retweet_count)
          * FROM tweets
          * WHERE text contains "Obama"
          *  GROUP BY user.id;
          */
        (df: DataFrame) => {
          df.groupBy($"id").sum("retweet_count").count()
        }
      case "13" =>
        /**
          * SELECT distinct user.id
          * FROM tweets
          * WHERE text contains @realDonaldTrump;
          */
        (df: DataFrame) => {
          df.distinct().count()
        }
      case _ =>
        // for most of the queries, we simply return the global count
        (df: DataFrame) => {
          df.count()
        }
    }

  }

}
