package flinktest

/**
  * Created by Administrator on 2016/12/5.
  */

import org.apache.flink.api.scala._

object PiEstimation {
  def main(args: Array[String]): Unit = {

//    val env = ExecutionEnvironment.createLocalEnvironment()
//    // Create initial DataSet
//    val initial = env.fromElements(0)
//    val count = initial.iterate(10000) { iterationInput: DataSet[Int] =>
//      val result = iterationInput.map { i =>
//        val x = Math.random()
//        val y = Math.random()
//        i + (if (x * x + y * y < 1) 1 else 0)
//      }
//      result
//    }
//
//    val result = count map { c => c / 10000.0 * 4 }
//    result.print()
//    env.execute("Iterative Pi Example");

    val numSamples: Long = if (args.length > 0) args(0).toLong else 1000000

    val env = ExecutionEnvironment.createLocalEnvironment(8)

    // count how many of the samples would randomly fall into
    // the upper right quadrant of the unit circle
    val count = env.generateSequence(1, numSamples).map  { sample =>
          val x = Math.random()
          val y = Math.random()
          if (x * x + y * y < 1) 1L else 0L
        }.reduce(_ + _)

    // ratio of samples in upper right quadrant vs total samples gives surface of upper
    // right quadrant, times 4 gives surface of whole unit circle, i.e. PI
    val pi = count.map ( _ * 4.0 / numSamples)

    println("We estimate Pi to be:")

    pi.print()

  }

}
