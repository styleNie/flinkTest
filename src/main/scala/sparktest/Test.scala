package sparktest

/**
  * Created by Administrator on 2016/10/8.
  */

import org.apache.spark.{SparkConf, SparkContext}
object Test {
  def main(args: Array[String]) {

    //    val x = Test(5)
    //    println(x)
    //
    //    x match
    //    {
    //      case Test(num) => println(x + " 是 " + num + " 的两倍！")
    //      //unapply 被调用
    //      case _ => println("无法计算")
    //    }
    //
    //  }
    //  def apply(x: Int) = x*2
    //  def unapply(z: Int): Option[Int] = if (z%2==0) Some(z/2) else None
    val conf = new SparkConf()
      .setMaster("local[8]")
      .setAppName("random shuffle")
      .set("spark.ui.port", "9995")
      .set("spark.driver.cores", "8")
      .set("spark.driver.memory", "5g")
      // .set("spark.executor.memory", "8g")
      //.set("spark.executor.num","8")
      .set("spark.rdd.compress", "true")

    val sc = new SparkContext(conf)

    val data = sc.textFile("D:\\2billion\\4\\part-*",9).map(x=>x.toInt).repartition(1).saveAsTextFile("file:///D:/twomillion")

    sc.stop()
  }
}
