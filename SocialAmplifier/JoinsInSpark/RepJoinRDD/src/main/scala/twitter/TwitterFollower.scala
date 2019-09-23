package twitter


import java.io.File

import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner
import org.apache.log4j.LogManager
import org.apache.spark.{SparkConf, SparkContext, sql}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType


/*
 * Scala object for calculating the number of followers a twitter user has.
 *
 */

case class TwitterFollower(follower: String, user: String)

object TwitterFollowerMain {

  def main(args: Array[String]) {

    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nwc.TwitterFollowerMain <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Twitter Follower")
    //    conf.set("spark.eventLog.enabled", "true")
    //    conf.set("spark.eventLog.dir", "log")
    val sc = new SparkContext(conf)


    // Delete output directory, only to ease local development; will not work on AWS. ===========

    //    val hadoopConf = new org.apache.hadoop.conf.Configuration
    //    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    //    try {
    //      hdfs.delete(new org.apache.hadoop.fs.Path(args(1)), true)
    //    } catch {
    //      case _: Throwable => {}
    //    }


    // take input from edges file
    val MAXFILTER = 50000
    val textFile = sc.textFile(args(0) + "/edges.csv")
    val rightRDD = textFile.map(line => (line.split(",")(0), line.split(",")(1))).filter(input => (input._1.toInt < MAXFILTER && input._2.toInt < MAXFILTER));

    val edgesMap = sc.broadcast(rightRDD.map{case(a, b) => (a, b)}.collectAsMap)

    val result = rightRDD.filter(res =>
      edgesMap.value.get(res._2).filter(z => !edgesMap.value.get(z).contains(res._1)).isEmpty).count()


    println(result/3)
    import java.io.PrintWriter
    new PrintWriter(new File("output")) {
      write(("Number of triangles :" + result/3)); close
    }

  }

//
//
//
//  def dataSet(dir: String): sql.DataFrame = {
//    val spark = SparkSession.builder().appName("Twitter Follower").getOrCreate()
//    import spark.implicits._
//    val schema = ScalaReflection.schemaFor[TwitterFollower].dataType.asInstanceOf[StructType]
//    val ds = spark.read.schema(schema).csv(dir + "/edges.csv").as[TwitterFollower]
//    val result = ds.groupBy("user").count()
//    println("=========================")
//    println(result.explain(true))
//    println("=========================")
//    return result
//  }
}