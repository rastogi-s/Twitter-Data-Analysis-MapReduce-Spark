package graph

import java.io.File

import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}


/*
 * Scala object for calculating diameter of a graph using Dataset.
 *
 */

object GraphDiameterMain {

  def main(args: Array[String]) {

    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nwc.graph diameter")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Graph Diameter")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().appName("Page Rank").getOrCreate()
    import spark.implicits._

    val textFile = sc.textFile(args(0) + "/edges.csv")

    val graph = textFile.map(line => (line.split(",")(0), line.split(",")(1)))

    graph.persist()

    val src1 = "2"
    val src2 = "3"

    var distances = graph.map(M => (M._1, (if (M._1 == src1) 0 else Int.MaxValue, if (M._1 == src2) 0 else Int.MaxValue))).reduceByKey((x, y) => x)

    var graphDF = graph.toDF("F", "T")
    var distancesDF = distances.map(vals => (vals._1, vals._2._1, vals._2._2)).toDF("N", "distance1", "distance2")

    var counter = distances.count()
    import org.apache.spark.sql.functions._
    while (counter > 0) {
      distancesDF = graphDF.join(distancesDF, $"F" === $"N").select($"T",
        when($"distance1" !== Int.MaxValue, $"distance1" + 1).otherwise($"distance1").as("distance1"),
        when($"distance2" !== Int.MaxValue, $"distance2" + 1).otherwise($"distance2").as("distance2"))
        .union(distancesDF)
        .groupBy("T").agg(min("distance1"), min("distance2"))
        .toDF("N", "distance1", "distance2")

      counter -= 1
    }

    distancesDF.collect().foreach(println)

    var src1LongestShortestPath = distancesDF.select("distance1").filter($"distance1" !== Int.MaxValue).agg(max("distance1"))
    var src2LongestShortestPath = distancesDF.select("distance2").filter($"distance2" !== Int.MaxValue).agg(max("distance2"))

    import java.io.PrintWriter
    new PrintWriter(new File("output")) {
      write(("Longest shortest Src1 path :" + src1LongestShortestPath.first().getInt(0) + "\n"));
      write(("Longest shortest Scr2 path :" + src2LongestShortestPath.first().getInt(0)));
      close
    }

  }

}

