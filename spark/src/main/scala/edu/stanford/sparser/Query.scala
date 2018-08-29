package edu.stanford.sparser

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

case class Query(format: String) {
  /** * Just parsing, filters, and projections
    */
  def queryStrToQueryParser(spark: SparkSession, queryStr: String): (String) => DataFrame = {
    import spark.implicits._

    queryStr match {
      /** *********** Zakir Queries *************/
      case "zakir1" =>

        /**
          * SELECT COUNT(*)
          * FROM  ipv4.20160425
          * WHERE p23.telnet.banner.banner is not NULL
          * AND   autonomous_system.asn = 9318;
          **/
        (input: String) => {
          spark.read.format(format).format(format).load(input).filter($"autonomous_system.asn" === 9318).filter(
            "p23.telnet.banner.banner is not null")
        }

      case "zakir2" =>

        /**
          * SELECT COUNT(*)
          * FROM  ipv4.20160425
          * WHERE p80.http.get.body CONTAINS 'content=\"WordPress 4.0';
          **/
        (input: String) => {
          spark.read.format(format).format(format).load(input).filter($"p80.http.get.body".contains("""content="WordPress 4.0"""))
        }

      case "zakir3" =>

        /**
          * SELECT COUNT(*)
          * FROM  ipv4.20160425
          * WHERE autonomous_system.asn = 2516;
          **/
        (input: String) => {
          spark.read.format(format).load(input).filter($"autonomous_system.asn" === 2516)
        }

      case "zakir4" =>

        /**
          * SELECT COUNT(*)
          * FROM  ipv4.20160425
          * WHERE location.country = "Chile"
          * AND   p80.http.get.status_code is not NULL;
          **/
        (input: String) => {
          spark.read.format(format).load(input).filter($"location.country" === "Chile").filter(
            "p80.http.get.status_code is not null")
        }

      case "zakir5" =>

        /**
          * SELECT COUNT(*)
          * FROM ipv4.20160425
          * WHERE p80.http.get.headers.server like '%DIR-300%';
          **/
        (input: String) => {
          spark.read.format(format).load(input).filter($"p80.http.get.headers.server".contains("DIR-300"))
        }

      case "zakir6" =>

        /**
          * SELECT COUNT(*)
          * FROM ipv4.20160425
          * WHERE p110.pop3s.starttls.banner is not NULL;
          **/
        (input: String) => {
          spark.read.format(format).load(input).filter("p110.pop3.starttls.banner is not null")
        }

      case "zakir7" =>

        /**
          * SELECT COUNT(*)
          * FROM ipv4.20160425
          * WHERE p21.ftp.banner.banner like '%Seagate Central Shared%';
          **/
        (input: String) => {
          spark.read.format(format).load(input).filter($"p21.ftp.banner.banner".contains("Seagate Central Shared"))
        }

      case "zakir8" =>

        /**
          * SELECT COUNT(*)
          * FROM ipv4.20160425
          * WHERE p20000.dnp3.status.support = true;
          **/
        (input: String) => {
          spark.read.format(format).load(input).filter($"p20000.dnp3.status.support" === true)
        }

      case "zakir9" =>

        /**
          * SELECT autonomous_system.asn, count(ipint) AS count
          * FROM ipv4.20160425
          * WHERE autonomous_system.name CONTAINS 'Verizon'
          * GROUP BY autonomous_system.asn;
          **/
        (input: String) => {
          spark.read.format(format).load(input).filter($"autonomous_system.name".contains("Verizon"))
            .select($"autonomous_system.asn", $"ipint")
        }

      case "zakir10" =>

        /**
          * SELECT COUNT(ip) as hosts,
          * p443.https.tls.certificate.parsed.fingerprint_sha256 AS certificate_fingerprint
          * FROM ipv4.20151201
          * WHERE p443.https.tls.certificate.parsed.issuer_dn CONTAINS "Let's Encrypt"
          * AND   p443.https.tls.validation.browser_trusted = true
          * GROUP BY certificate_fingerprint ORDER BY hosts DESC;
          **/
        (input: String) => {
          spark.read.format(format).load(input).filter($"p443.https.tls.certificate.parsed.issuer_dn"
            .contains("Let's Encrypt"))
            .filter($"p443.https.tls.validation.browser_trusted" === true)
            .select($"p443.https.tls.certificate.parsed.fingerprint_sha256", $"ipint")
        }

      /** *********** Twitter Queries *************/
      case "twitter1" =>

        /**
          * SELECT count(*)
          * FROM tweets
          * WHERE text contains "Donald Trump"
          * AND created_at contains "Sep 13";
          **/
        (input: String) => {
          spark.read.format(format).load(input).filter($"text".contains("Donald Trump") &&
            $"created_at".contains("Sep 13"))
        }

      case "twitter2" =>

        /**
          * SELECT user.id, SUM(retweet_count)
          * FROM tweets
          * WHERE text contains "Obama"
          *  GROUP BY user.id;
          **/
        (input: String) => {
          spark.read.format(format).load(input).filter($"text".contains("Obama"))
            .select($"user.id", $"retweet_count")
        }

      case "twitter3" =>

        /**
          * SELECT id
          * FROM tweets
          * WHERE user.lang = "msa";
          **/
        (input: String) => {
          spark.read.format(format).load(input).filter($"user.lang" === "msa").select($"id")
        }

      case "twitter4" =>

        /**
          * SELECT distinct user.id
          * FROM tweets
          * WHERE text contains @realDonaldTrump;
          **/
        (input: String) => {
          spark.read.format(format).load(input).filter($"text".contains("@realDonaldTrump"))
            .select($"user.id")
        }

      /** TPCH Queries. Taken from https://github.com/ssavvides/tpch-spark */

        /*
      case "tpch3" =>
        /**
         *
						select
								l_orderkey,
								sum(l_extendedprice * (1 - l_discount)) as revenue,
								o_orderdate,
								o_shippriority
							from
											customer,
											orders,
											lineitem
							where
											c_mktsegment = 'BUILDING'
											and c_custkey = o_custkey
											and l_orderkey = o_orderkey
											and o_orderdate < date '1995-03-15'
											and l_shipdate > date '1995-03-15'
							group by
											l_orderkey,
											o_orderdate,
											o_shippriority
							order by
											revenue desc,
											o_orderdate
							limit 10
          **/
        (input: String) => {
					customer.filter($"c_mktsegment" === "BUILDING").select($"c_custkey")
        }
        */
    }
  }

  /**
    * The query itself: could be a COUNT, GROUP BY, DISTINCT, ORDER BY, etc. Return
    * the number of rows of the final dataframe (i.e., call count())
    */
  def queryStrToQuery(spark: SparkSession, queryStr: String): (DataFrame) => Long = {
    import spark.implicits._

    queryStr match {
      case "zakir9" =>

        /**
          * SELECT autonomous_system.asn, count(ipint) AS count
          * FROM ipv4.20160425
          * WHERE autonomous_system.name CONTAINS 'Verizon'
          * GROUP BY autonomous_system.asn;
          */
        (df: DataFrame) => {
          df.groupBy($"asn").count().count()
        }
      case "zakir10" =>

        /**
          * SELECT COUNT(ip) as hosts,
          * p443.https.tls.certificate.parsed.fingerprint_sha256 AS certificate_fingerprint
          * FROM ipv4.20151201
          * WHERE p443.https.tls.certificate.parsed.issuer_dn CONTAINS "Let's Encrypt"
          * AND   p443.https.tls.validation.browser_trusted = true
          * GROUP BY certificate_fingerprint ORDER BY hosts DESC;
          **/
        (df: DataFrame) => {
          df.groupBy("fingerprint_sha256").count()
            .withColumnRenamed("count", "hosts")
            .orderBy($"hosts".desc).count()
        }
      case "twitter2" =>

        /**
          * SELECT user.id, SUM(retweet_count)
          * FROM tweets
          * WHERE text contains "Obama"
          *  GROUP BY user.id;
          */
        (df: DataFrame) => {
          df.groupBy($"id").sum("retweet_count").count()
        }
      case "twitter4" =>

        /**
          * SELECT distinct user.id
          * FROM tweets
          * WHERE text contains @realDonaldTrump;
          */
        (df: DataFrame) => {
          df.distinct().count()
        }

        /*
      case "tpch3" =>
        /**
         *
						select
								l_orderkey,
								sum(l_extendedprice * (1 - l_discount)) as revenue,
								o_orderdate,
								o_shippriority
							from
											customer,
											orders,
											lineitem
							where
											c_mktsegment = 'BUILDING'
											and c_custkey = o_custkey
											and l_orderkey = o_orderkey
											and o_orderdate < date '1995-03-15'
											and l_shipdate > date '1995-03-15'
							group by
											l_orderkey,
											o_orderdate,
											o_shippriority
							order by
											revenue desc,
											o_orderdate
							limit 10
          **/
        (fcust: DataFrame) => {

					val forders = order.filter($"o_orderdate" < "1995-03-15")
					val flineitems = lineitem.filter($"l_shipdate" > "1995-03-15")

					fcust.join(forders, $"c_custkey" === forders("o_custkey"))
						.select($"o_orderkey", $"o_orderdate", $"o_shippriority")
						.join(flineitems, $"o_orderkey" === flineitems("l_orderkey"))
						.select($"l_orderkey",
							decrease($"l_extendedprice", $"l_discount").as("volume"),
							$"o_orderdate", $"o_shippriority")
								.groupBy($"l_orderkey", $"o_orderdate", $"o_shippriority")
								.agg(sum($"volume").as("revenue"))
								.sort($"revenue".desc, $"o_orderdate")
								.limit(10)
								.distinct()
								.count()
        }
         */

      case _ =>
        // for most of the queries, we simply return the global count
        (df: DataFrame) => {
          df.count()
        }
    }
  }

  def queryStrToSchema(queryStr: String): StructType = {
    queryStr match {
      case "zakir9" =>

        /**
          * SELECT autonomous_system.asn, count(ipint) AS count
          * FROM ipv4.20160425
          * WHERE autonomous_system.name CONTAINS 'Verizon'
          * GROUP BY autonomous_system.asn;
          */
        new StructType().add("asn", IntegerType).add("ipint", IntegerType)
      case "zakir10" =>

        /**
          * SELECT COUNT(ip) as hosts,
          * p443.https.tls.certificate.parsed.fingerprint_sha256 AS certificate_fingerprint
          * FROM ipv4.20151201
          * WHERE p443.https.tls.certificate.parsed.issuer_dn CONTAINS "Let's Encrypt"
          * AND   p443.https.tls.validation.browser_trusted = true
          * GROUP BY certificate_fingerprint ORDER BY hosts DESC;
          **/
        new StructType().add("fingerprint_sha256", StringType, nullable = false,
          Metadata.fromJson("""{"length": 64}"""))
          .add("ipint", IntegerType)
      case "twitter2" =>

        /**
          * SELECT user.id, SUM(retweet_count)
          * FROM tweets
          * WHERE text contains "Obama"
          *  GROUP BY user.id;
          */
        new StructType().add("id", LongType).add("retweet_count", IntegerType)
      case "twitter4" =>

        /**
          * SELECT distinct user.id
          * FROM tweets
          * WHERE text contains @realDonaldTrump;
          */
        new StructType().add("id", LongType)
      case _ =>
        // Default value for every other query
        new StructType().add("value", LongType)
    }
  }
}

object Query {
  val sparserQueryMap = Map(
    "zakir1" -> "0",
    "zakir2" -> "1",
    "zakir3" -> "2",
    "zakir4" -> "3",
    "zakir5" -> "4",
    "zakir6" -> "5",
    "zakir7" -> "6",
    "zakir8" -> "7",
    "zakir9" -> "8",
    "zakir10" -> "9",
    "twitter1" -> "10",
    "twitter2" -> "11",
    "twitter3" -> "12",
    "twitter4" -> "13")
}
