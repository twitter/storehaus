package com.twitter.storehaus.asynchbase

import com.stumbleupon.async.Deferred
import com.twitter.storehaus.testing.generator.NonEmpty.alphaStr
import com.twitter.util.{Return, Throw, Await}
import org.scalacheck.Prop.forAll
import org.scalacheck.Properties

class DeferredToFutureConverterProperties extends Properties("DeferredToFutureConverter") {
  import DeferredToFutureConverter.toFuture
  property("convert Deferred to Future with result") = forAll(alphaStr) { str =>
    val d = Deferred.fromResult[String](str)
    val f = toFuture(d)
    Await.result(f.liftToTry) == Return(str)
  }
  property("convert Deferred to Future with exception") = forAll(alphaStr) { str =>
    val ex = new Exception(str)
    val d = Deferred.fromError[String](ex)
    val f = toFuture(d)
    Await.result(f.liftToTry) == Throw(ex)
  }
}
