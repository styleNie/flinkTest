package sparktest

/**
  * Created by Administrator on 2016/10/8.
  */

import java.util.Random

import org.apache.spark._

object RandomProduce {
  def main(args:Array[String]): Unit ={
    val conf=new SparkConf()
      .setMaster("local[8]")
      .setAppName("random shuffle")
      .set("spark.ui.port", "9995")
      .set("spark.driver.cores", "8")
      .set("spark.driver.memory", "4g")
     // .set("spark.executor.memory", "8g")
      //.set("spark.executor.num","8")
      .set("spark.rdd.compress", "true")

    val sc = new SparkContext(conf)

    val rand = new Random()

    val n = 400000000L
    val slices = 100
    val num1 = sc.parallelize(1L until  n, slices).map { i =>
      rand.nextInt(1000)
    }
    val num2 = sc.parallelize(1L until  n, slices).map { i =>
        rand.nextInt(1000)
    }
    val num3 = sc.parallelize(1L until  n, slices).map { i =>
      rand.nextInt(1000)
    }
    val num = num1.union(num2).union(num3).repartition(20).saveAsTextFile("D:\\sixmillion")

    sc.stop()
  }
}
