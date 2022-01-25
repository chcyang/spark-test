package io.github.chcyang.spark

import scala.reflect.ClassTag
import org.{tensorflow => tf}

object ScalaReflect {

  import scala.reflect.runtime.{universe => ru}

  def main(args: Array[String]): Unit = {

    val m = Array[Int](1, 2)

    val valueType = getTypeTag(m).tpe
    println(valueType)


    test(Array(Array(123,234)))
  }

  def getTypeTag[T: ru.TypeTag](obj: T) = ru.typeTag[T]

  def test[T: ClassTag](features: T) = {

    //    val valueType = getTypeTag(features).tpe
    //    println(valueType)

    println(features)

    val x = tf.Tensor.create(Array(features))
    println(x.numDimensions())

    x.close()
  }
}
