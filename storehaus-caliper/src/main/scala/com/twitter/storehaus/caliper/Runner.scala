package com.twitter.storehaus.caliper

import com.google.caliper.{Runner => CaliperMain}
import com.google.common.collect.ObjectArrays.concat

import java.io.PrintWriter

object Runner {

  def main(args: Array[String]) {
    CaliperMain.main(args)
  }

  // def main(args: Array[String]) {
  //   val th = if(args.size > 0 && args(0) == "warmed") com.ichi.bench.Thyme.warmed(verbose = print) else new com.ichi.bench.Thyme
  //   val fn = new WriteThroughCacheBenchmark
  //   fn.numInputKeys = 500
  //   fn.numElements = 100000
  //   println("Starting set up")
  //   fn.setUp
  //   println("Starting benchmark")
  //   println(th.pbench(fn.timeDoUpdates(10)))
  //   println("Finished benchmark")
  //   System.exit(1)
  // }

}
