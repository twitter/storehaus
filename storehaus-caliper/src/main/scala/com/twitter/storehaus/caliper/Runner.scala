package com.twitter.storehaus.caliper

import com.google.caliper.{Runner => CaliperMain}
import com.google.common.collect.ObjectArrays.concat

import java.io.PrintWriter

object Runner {

  def main(args: Array[String]) {
    CaliperMain.main(args)
  }

}
