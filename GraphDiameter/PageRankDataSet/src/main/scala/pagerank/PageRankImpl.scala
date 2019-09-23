package pagerank

import org.apache.log4j.LogManager
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SparkSession}

import scala.collection.mutable.ListBuffer


/*
 * Scala object for calculating page rank using dataset or dataframe.
 *
 */

object PageRankImplMain {

  def main(args: Array[String]) {

    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nwc.PageRankingMain")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Page Rank")
    conf.set("spark.eventLog.enabled", "true")
    conf.set("spark.eventLog.dir", "log")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().appName("Page Rank").getOrCreate()
    import spark.implicits._

    val K = 100

    var pageRankList = new ListBuffer[(Int, Double)]()
    var listOfEdges = new ListBuffer[(Int, Int)]()
    pageRankList = pageRankList :+ (0, 0.0)
    val totalNodes = K * K
    for (index <- 1 to totalNodes) {
      if (index % 3 == 0)
        listOfEdges = listOfEdges :+ (index, 0)
      else
        listOfEdges = listOfEdges :+ (index, index + 1)

      val res = 1 / totalNodes.toDouble
      pageRankList = pageRankList :+ (index, res.toDouble)
    }


    val edgeDF = sc.parallelize(listOfEdges).toDF("v1", "v2").alias("edges")
    var pageRankDF = sc.parallelize(pageRankList).toDF("v1", "pr").alias("rank")
    var convergence = new ListBuffer[(Int, Double)]()
    for (index <- 1 to 10) {
      // join graph and ranks
      val tripleDF = edgeDF.join(pageRankDF, $"edges.v1" === $"rank.v1").select("edges.v1", "edges.v2", "rank.pr")

      // map values (v2,pr)
      val temp = tripleDF.select("v2", "pr")

      // group by v2 and sum up pr values in each grp.
      val temp2 = temp.groupBy("v2").sum().select("v2", "sum(pr)").toDF("v2", "pr").alias("temp2")

      // delta calulation
      val delta = temp2.filter($"v2" === 0).first().get(1).asInstanceOf[Double]

      //correct the loss of edges with zero incoming edges.
      val correctedDF = edgeDF.join(temp2, $"edges.v1" === $"temp2.v2", "left_outer").toDF("v1", "v2", "v2_2", "pr")

      import org.apache.spark.sql.functions._
      pageRankDF = correctedDF.withColumn("newPR", when(col("pr").isNotNull, col("pr")).otherwise(0.0)).select("v1", "newPR")

      val res = delta / totalNodes.toDouble
      pageRankDF = pageRankDF.withColumn("pr", when(col("v1") === 0.0, col("newPR")).otherwise(col("newPR") + res)).drop("newPR")

      var conv = pageRankDF.agg(sum("pr")).first().get(0).asInstanceOf[Double]
      convergence = convergence :+ (index, conv)

      pageRankDF = pageRankDF.toDF("v1", "pr").alias("rank")

    }

    println("======= Convergence at each level =========")
    convergence.foreach(println)
    println("======= Convergence at each level =========")

    println("====final page rank for 100 vertices ====")
    println(pageRankDF.explain(true))
    pageRankDF = pageRankDF.sort(org.apache.spark.sql.functions.col("pr").desc).limit(100)
    println("====final page rank for 100 vertices ====")
    println(pageRankDF.select("*").show())


  }


}

