package io.github.chcyang.spark.test

object Flatten {

  def main(args: Array[String]): Unit = {

    val list: List[List[Int]] = List(List(1, 2, 3), List(5, 7))
    val flattenList: List[Int] = list.flatten
    flattenList.foreach(println)
  }
}
