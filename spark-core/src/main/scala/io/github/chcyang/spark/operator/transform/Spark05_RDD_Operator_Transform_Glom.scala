package io.github.chcyang.spark.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark05_RDD_Operator_Transform_Glom {
  def main(args: Array[String]): Unit = {

    // 创建环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val list = List(1, 2, 3, 4, 5, 6)

    val rdd: RDD[Int] = sc.makeRDD(list, 2)

    //    val resultRDD = rdd.glom().map {
    //      array => array.max
    //    }.reduce(_ + _)
    //
    //    println(resultRDD)

    val maxRDD: RDD[Int] = rdd.glom().map(_.max)

    println(maxRDD.collect().sum)

    sc.stop()

  }

}
