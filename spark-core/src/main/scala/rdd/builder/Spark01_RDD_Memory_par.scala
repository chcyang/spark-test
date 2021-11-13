package rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Memory_par {

  def main(args: Array[String]): Unit = {
    // 创建环境

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)
    //创建RDD
    val seq = Seq[Int](1, 2, 3, 4)

    //    val rdd: RDD[Int] = sc.parallelize(seq)
    val rdd: RDD[Int] = sc.makeRDD(seq,3)

    rdd.saveAsTextFile("output")
    // 关闭环境
    sc.stop()
  }
}
