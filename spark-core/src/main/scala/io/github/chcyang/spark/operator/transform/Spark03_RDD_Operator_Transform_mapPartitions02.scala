package io.github.chcyang.spark.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_Operator_Transform_mapPartitions02 {
  def main(args: Array[String]): Unit = {

    // 创建环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val list = List(1, 2, 3, 4, 5)

    val rdd: RDD[Int] = sc.makeRDD(list, 2)

    val rdd2: RDD[(Int, Int)] = rdd.mapPartitionsWithIndex {
      (index, iter) =>
        iter.map {
          data => (index, data)
        }
    }

    rdd2.collect().foreach(println)

    sc.stop()

  }

}
