package github.chcyang.spark.mllib

import org.apache.spark.sql.SparkSession

import scala.collection.mutable.WrappedArray
import org.{tensorflow => tf}

object LoadTensorflowModel {


  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("TfDataFrame")
      .getOrCreate()

    val sc = spark.sparkContext

    import spark.implicits._

    val bundle = tf.SavedModelBundle
      .load("/Users/neco/data/linear_model/1", "serve")

    val broads = sc.broadcast(bundle)

    //构造预测函数，并将其注册成sparkSQL的udf
    val tfpredict = (features: WrappedArray[Float]) => {
      val bund = broads.value
      val sess = bund.session()
      val x = tf.Tensor.create(Array(features.toArray))
      val y = sess.runner().feed("serving_default_inputs:0", x)
        .fetch("StatefulPartitionedCall:0").run().get(0)
      val result = Array.ofDim[Float](y.shape()(0).toInt, y.shape()(1).toInt)
      y.copyTo(result)
      val y_pred = result(0)(0)
      y_pred
    }
    spark.udf.register("tfpredict", tfpredict)

    //构造DataFrame数据集，将features放到一列中
    val dfdata = sc.parallelize(List(Array(1.0f, 2.0f), Array(3.0f, 5.0f), Array(7.0f, 8.0f))).toDF("features")
    dfdata.show

    //调用sparkSQL预测函数，增加一个新的列作为y_preds
    val dfresult = dfdata.selectExpr("features", "tfpredict(features) as y_preds")
    dfresult.show
    bundle.close

  }

}
