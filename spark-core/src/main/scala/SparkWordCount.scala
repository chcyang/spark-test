package main.scala

import org.apache.spark.{ SparkConf, SparkContext }

object SparkWordCount {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("WordCount").setMaster("local")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("data.txt")
    val pairs = lines.flatMap(_.split(" ")).map(s => (s, 1))
    pairs
      .groupBy((n: (String, Int)) => n._1, 3)
      .mapValues(_.map(_._2))
      .map(x => (x._1, x._2.reduce(_ + _)))
      .collect()
      .foreach(println)
//    val counts = pairs.reduceByKey((a, b) => a + b).collect()
//    counts.foreach(println)
    sc.stop()

  }

}
