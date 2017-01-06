package sparktest

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2016/10/8.
  */
object FindMax {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[8]")
      .setAppName("random shuffle")
      .set("spark.ui.port", "9995")
      .set("spark.driver.cores", "8")
      .set("spark.driver.memory", "6g")
      // .set("spark.executor.memory", "8g")
      //.set("spark.executor.num","8")
      .set("spark.rdd.compress", "true")

    val sc = new SparkContext(conf)

    val start = System.currentTimeMillis()
    //val maxNum = sc.textFile("E:\\hadoop\\data\\billionOfFive",104).map(_.toInt).max()
    implicit val valueOrdering = new Ordering[(String,Int)]{
      override def compare(x:(String,Int),y:(String,Int)):Int={
        x._2.compareTo(y._2)
      }
    }
    val maxNum = sc.textFile("file:///D:/6billion/part-*",368).map(x=>(x,1)).reduceByKey(_+_).max()(valueOrdering)
    println(maxNum)

    val end = System.currentTimeMillis()
    printf("Time cost: %d.\n",(end-start)/1000)

  }
}
