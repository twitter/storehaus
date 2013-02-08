package com.twitter.storehaus

import org.scalacheck.Properties

object ReplicatedStoreProperties extends Properties("ReplicatedStore") {
  import StoreProperties.storeTest

  property("ReplicatedStore test") =
    storeTest[ReplicatedStore[MapStore[String, Int], String, Int], String, Int](new ReplicatedStore(
        Stream.continually(new MapStore[String, Int]()).take(100).toSeq))
}