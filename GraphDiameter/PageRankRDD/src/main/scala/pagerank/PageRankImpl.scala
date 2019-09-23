package pagerank

import org.apache.log4j.LogManager
import org.apache.spark.{SparkConf, SparkContext}


import scala.collection.mutable.ListBuffer


/*
 * Scala object for calculating page rank .
 *
 */

object PageRankImplMain {

  def main(args: Array[String]) {

    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nwc.PageRankingMain <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Page Rank")
    conf.set("spark.eventLog.enabled", "true")
    conf.set("spark.eventLog.dir", "log")
    val sc = new SparkContext(conf)

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


    val edgeRDD = sc.parallelize(listOfEdges)
    var pageRankRDD = sc.parallelize(pageRankList)

    var conv = 0.0
    var convergence = new ListBuffer[(Int, Double)]()
    for (index <- 1 to 10) {
      // join graph and ranks
      val tripleRDD = edgeRDD.join(pageRankRDD).map(vals => (vals._1, vals._2._1, vals._2._2))

      // map values (v2,pr)
      val temp = tripleRDD.map(vals => (vals._2, vals._3))

      // group by v2 and sum up pr values in each grp.
      val temp2 = temp.reduceByKey(_ + _)

      // delta calulation
      val delta = temp2.filter(vals => vals._1 == 0).collect().toList.head._2

      // correct the loss of edges with zero incoming edges.
      val correctedRDD = edgeRDD.leftOuterJoin(temp2).map(vals => (vals._1, if (vals._2._2 == None) 0.toDouble else vals._2._2.get))

      //
      pageRankRDD = correctedRDD.map(vals => (vals._1, if (vals._1 == 0) vals._2 else (vals._2 + (delta / totalNodes))))
      conv = pageRankRDD.map(vals => vals._2).sum()

      println("loop number -----> " + index + "convergence ---->" + conv)
      convergence = convergence :+ (index, conv)

      println("======== summing of convergence======")
      println(pageRankRDD.toDebugString)

    }

    println("======= Convergence at each level =========")
    convergence.foreach(println)
    println("======= Convergence at each level =========")

    println("====final page rank for 100 vertices ====")
    pageRankRDD.sortBy(_._2, false).take(100).foreach(println)
    println("====final page rank for 100 vertices ====")

    pageRankRDD.saveAsTextFile("output")

  }


}

