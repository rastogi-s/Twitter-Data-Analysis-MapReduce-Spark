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

case class TwitterFollower(follower: Int, user: Int)

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
    conf.set("spark.sql.crossJoin.enabled", "true")


    // Delete output directory, only to ease local development; will not work on AWS. ===========

    //    val hadoopConf = new org.apache.hadoop.conf.Configuration
    //    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    //    try {
    //      hdfs.delete(new org.apache.hadoop.fs.Path(args(1)), true)
    //    } catch {
    //      case _: Throwable => {}
    //    }


    // take input from edges file

    val MAXFILTER = 10
    val textFile = sc.textFile(args(0) + "/edges.csv")
    val result = dataSet(args(0)).count / 3

    import java.io.PrintWriter
    new PrintWriter(new File("output")) {
      write(("Number of triangles :" + result));
      close
    }

  }


  def dataSet(dir: String): sql.DataFrame = {
    val MAXFILTER = 10
    val spark = SparkSession.builder().appName("Twitter Follower").getOrCreate()
    import spark.implicits._

    val schema = ScalaReflection.schemaFor[TwitterFollower].dataType.asInstanceOf[StructType]
    val ds = spark.read.schema(schema).csv(dir + "/edges.csv").as[TwitterFollower]
    val leftDS = ds.filter($"user" < MAXFILTER && $"follower" < MAXFILTER).toDF("f1", "u1")
    val rightDS = ds.filter($"user" < MAXFILTER && $"follower" < MAXFILTER).toDF("f2", "u2")
    val twoPath = leftDS.join(rightDS, $"u1" === $"f2").drop("u1", "f2")
    val edgesDS = ds.filter($"user" < MAXFILTER && $"follower" < MAXFILTER).toDF("f3", "u3")


    println("=========================")
    println("Two Path")
    println(twoPath.explain(true))
    println("=========================")
    val result = twoPath.join(edgesDS, $"f1" === $"u3" && $"u2" === $"f3")

    println("=========================")
    println("triangle")
    println(result.explain(true))
    println("=========================")

    return result
  }
}