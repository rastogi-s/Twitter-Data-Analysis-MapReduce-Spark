package graph

import java.io.File

import org.apache.log4j.LogManager
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer


/*
 * Scala object for calculating diameter of a graph using RDD.
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

    val textFile = sc.textFile(args(0) + "/edges.csv")

    val graph = textFile.map(line => (line.split(",")(0), line.split(",")(1))).groupBy(input => input._1).map(vals => (vals._1, (vals._2.flatMap(x => x._2))))
    graph.persist()
    val src1 = "2"
    val src2 = "3"
    var distances = graph.map(M => (M._1, (if (M._1 == src1) 0 else Int.MaxValue, if (M._1 == src2) 0 else Int.MaxValue)))
    var counter = distances.count()

    while (counter > 0) {
      distances = graph.join(distances).flatMap(vals => extractVertices(vals._1, vals._2._1.map(x => x.toString).toList,
        vals._2._2._1, vals._2._2._2)).reduceByKey((x, y) => (Math.min(x._1, y._1), Math.min(x._2, y._2)))
      counter -= 1
    }

    distances.foreach(println)

    var src1LongestShortestPath = distances.max()(new Ordering[Tuple2[String, (Int,Int)]]() {
      override def compare(x: (String,(Int,Int)), y: (String, (Int,Int))): Int =
        Ordering[Int].compare(x._2._1, y._2._1)
    })
    var src2LongestShortestPath =distances.max()(new Ordering[Tuple2[String, (Int,Int)]]() {
      override def compare(x: (String,(Int,Int)), y: (String, (Int,Int))): Int =
        Ordering[Int].compare(x._2._2, y._2._2)
    })
    println(src1LongestShortestPath)
    println(src2LongestShortestPath)
    import java.io.PrintWriter
    new PrintWriter(new File("output")) {
      write(("Longest shortest Src1 path :" + src1LongestShortestPath._2._1 + "\n"));
      write(("Longest shortest Scr2 path :" +src2LongestShortestPath._2._2));
      close
    }

  }

  def extractVertices(parentNode: String, adjList: List[String], distanceSrc1: Int, distanceSrc2: Int)
  : ListBuffer[(String, (Int, Int))] = {

    var list = new ListBuffer[(String, (Int, Int))]
    list.append((parentNode, (distanceSrc1, distanceSrc2)))

    adjList.foreach(node => list.append((node,
      (if (distanceSrc1 == Int.MaxValue) distanceSrc1 else distanceSrc1 + 1,
        if (distanceSrc2 == Int.MaxValue) distanceSrc2 else distanceSrc2 + 1))))

    return list

  }

}

