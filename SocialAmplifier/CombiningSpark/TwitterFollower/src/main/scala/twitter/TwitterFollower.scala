package twitter

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
    conf.set("spark.eventLog.enabled", "true")
    conf.set("spark.eventLog.dir", "log")
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
    val textFile = sc.textFile(args(0) + "/edges.csv")

    val result = reduceByRDD(textFile)
    // val result = groupByRDD(textFile)
    //val result = aggregateRDD(textFile)
    //val result = foldByKeyRDD(textFile)
    //val result = dataSet(args(0))
    //result.rdd.map(_.toString()).saveAsTextFile(args(1) + "/dataSet")
    result.saveAsTextFile(args(1) + "/reduceByRDD")
    //result.rdd.map(_.toString()).write.text(args(1) + "/dataSet/xyz.txt")

  }


  def reduceByRDD(textFile: RDD[String]): RDD[(String, Int)] = {
    val result = textFile.map(line => (line.split(",")(1), 1)).reduceByKey(_ + _)
    println("=========================")
    println(result.toDebugString)
    println("=========================")
    return result
  }

  def groupByRDD(textFile: RDD[String]): RDD[(String, Int)] = {
    val result = textFile.map(line => (line.split(",")(1), 1)).groupByKey().map(data => (data._1, data._2.sum))
    println(result.toDebugString)
    return result
  }

  def aggregateRDD(textFile: RDD[String]): RDD[(String, Int)] = {
    val result = textFile.map(line => (line.split(",")(1), 1)).aggregateByKey(0)(_ + _, _ + _)
    println(result.toDebugString)
    return result
  }

  def foldByKeyRDD(textFile: RDD[String]): RDD[(String, Int)] = {
    val result = textFile.map(line => (line.split(",")(1), 1)).foldByKey(0)(_ + _)
    println(result.toDebugString)
    return result
  }


  def dataSet(dir: String): sql.DataFrame = {
    val spark = SparkSession.builder().appName("Twitter Follower").getOrCreate()
    import spark.implicits._
    val schema = ScalaReflection.schemaFor[TwitterFollower].dataType.asInstanceOf[StructType]
    val ds = spark.read.schema(schema).csv(dir + "/edges.csv").as[TwitterFollower]
    val result = ds.groupBy("user").count()
    println("=========================")
    println(result.explain(true))
    println("=========================")
    return result
  }
}