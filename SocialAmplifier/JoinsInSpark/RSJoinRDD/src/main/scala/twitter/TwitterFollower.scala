package twitter


import java.io.File

import org.apache.log4j.LogManager
import org.apache.spark.{SparkConf, SparkContext, sql}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType


/*
 * Scala object for calculating the number of triangle relations between users .
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


    val MAXFILTER = 50000
    val textFile = sc.textFile(args(0) + "/edges.csv")
    val leftRDD = textFile.map(line => (line.split(",")(1), line.split(",")(0))).filter(input => (input._1.toInt < MAXFILTER && input._2.toInt < MAXFILTER));
    val rightRDD = textFile.map(line => (line.split(",")(0), line.split(",")(1))).filter(input => (input._1.toInt < MAXFILTER && input._2.toInt < MAXFILTER));
    val twoPath = joinOnKey(leftRDD, rightRDD).map(res => res._2).map(res => (res, "1"))
    val edges = leftRDD.map(res => (res, "1"))
    val result = joinOnCompositeKey(edges, twoPath).map(res => res._1).count / 3
    import java.io.PrintWriter
    new PrintWriter(new File("output")) {
      write(("Number of triangles :" + result)); close
    }

  }

  def joinOnKey(leftRDD: RDD[(String, String)], rightRDD: RDD[(String, String)])
  : RDD[(String, (String, String))] = {
    return leftRDD.join(rightRDD)
  }

  def joinOnCompositeKey(leftRDD: RDD[((String, String), String)], rightRDD: RDD[((String, String), String)])
  : RDD[((String, String), (String, String))] = {
    return leftRDD.join(rightRDD)
  }
  

}
