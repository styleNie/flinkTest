package flinktest

/**
  * Created by Administrator on 2016/12/5.
  */

import org.apache.flink.api.scala._
object findMaxNum {
  def main(args: Array[String]): Unit = {
    val start = System.currentTimeMillis()
    val env = ExecutionEnvironment.createLocalEnvironment(8)
  //env.setParallelism(48)
    val text = env.readTextFile("file:///D:/6billion/")

    val maxCount = text.map((_,1)).groupBy(0).sum(1).maxBy(1)
    maxCount.print()

    val end = System.currentTimeMillis()
    printf("Time cost: %d.\n",(end-start)/1000)
  }

}
