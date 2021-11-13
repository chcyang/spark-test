package operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Operator_Transform_Flatmap {
  def main(args: Array[String]): Unit = {

    // 创建环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val list = List(List(1, 2, 3), List(4, 5))

    val rdd: RDD[List[Int]] = sc.makeRDD(list, 2)

    val flatRdd: RDD[Int] = rdd.flatMap {
      list => list
    }

    flatRdd.collect().foreach(println)

    sc.stop()

  }

}
