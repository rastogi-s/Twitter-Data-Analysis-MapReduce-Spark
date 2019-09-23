package wc

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level

/*
 * Scala object for calculating the number of followers a twitter user has.
 *
 */

object TwitterFollowerMain {

  def main(args: Array[String]) {

    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nwc.TwitterFollowerMain <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Twitter Follower")
    val sc = new SparkContext(conf)


    // Delete output directory, only to ease local development; will not work on AWS. ===========

//    val hadoopConf = new org.apache.hadoop.conf.Configuration
//    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
//    try {
//      hdfs.delete(new org.apache.hadoop.fs.Path(args(1)), true)
//    } catch {
//      case _: Throwable => {}
//    }


    // take input from nodes file
    val textFile1 = sc.textFile(args(0) + "/nodes.csv")
    // take input from edges file
    val textFile2 = sc.textFile(args(0) + "/edges.csv")
    val counts1 = textFile1.map(line => (line, 0)).reduceByKey(_ + _)
    val counts2 = textFile2.map(line => (line.split(",")(1), 1)).reduceByKey(_ + _)
    val result = (counts2 ++ counts1).reduceByKey(_ + _)
    //println(result.toDebugString)
    result.saveAsTextFile(args(1))
  }
}