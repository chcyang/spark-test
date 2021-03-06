package io.github.chcyang.spark.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_File_par {

  def main(args: Array[String]): Unit = {
    // 创建环境

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)
    //创建RDD
    val rdd: RDD[String] = sc.textFile("data1.txt")

    rdd.collect().foreach(println)

    rdd.saveAsTextFile("data/result")
    // 关闭环境
    sc.stop()
  }
}
