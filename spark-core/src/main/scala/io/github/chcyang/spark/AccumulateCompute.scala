package io.github.chcyang

package

object spark {

  def main(args: Array[String]): Unit = {

    var total = 0.0
    (1 to 20).map {
      a =>
        println(a)
        total = (total + 5000) * 1.03
    }

    println(total)
  }
}
