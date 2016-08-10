package com.twitter.storehaus

import com.twitter.util.{Duration, Future, Try, Throw, Return, TimeoutException}

import java.util.concurrent.atomic.{AtomicReference => AtomicRef, AtomicBoolean}

object FutureWithDefault {
  def const[T](t: Try[T]) = new FutureWithDefault(Future.const(t)) {
      def onTimeout = t
    }

  def value[T](t: T) = const(Return(t))

  def onTimeout[T](f: Future[T])(to: => Try[T]) =
    new FutureWithDefault(f) { lazy val onTimeout = to }
}

/**
 *
 */
abstract class FutureWithDefault[+T](val future: Future[T]) {
  // This is what you override:
  protected def onTimeout: Try[T]

  ////////////////////////////////////////////
  // Be super careful about changing anything below
  ////////////////////////////////////////////

  protected val timedoutRef = new AtomicBoolean(false)
  def timedout: Boolean = timedoutRef.get

  // the variance is messing up the types, here, casting for now
  protected def process(t: Try[_]): Try[_] = {
    t.rescue {
      case (t: TimeoutException) => {
        // We're done:
        future.cancel
        timedoutRef.set(true)
        onTimeout
      }
      case (x: Throwable) => {
        if(future.isCancelled) {
          timedoutRef.set(true)
          onTimeout
        }
        else {
          Throw(x)
        }
      }
    }
  }

  /** Wait a period of time, or return the timeout value
   * if you get a timeout, the future is canceled.
   * always returns the same value
   */
  def get(d: Duration): Try[T] = process(future.get(d)).asInstanceOf[Try[T]]

  /** If the result is ready, get it, otherwise get the timeout
   */
  def getNow: Try[T] =
    future
      .poll.map { t => process(t) }
      .getOrElse {
        future.cancel
        timedoutRef.set(true)
        onTimeout
      }.asInstanceOf[Try[T]]

  def flatMap[U](fn: T => FutureWithDefault[U]): FutureWithDefault[U] = {
    val resref = new AtomicRef[() => Try[U]]() // Initially null
    val fnref = new AtomicRef[T => FutureWithDefault[U]](fn)

    val nextFuture = future.flatMap { t =>
      // Get the future, if this is the first call:
      if (fnref.compareAndSet(fn, null)) {
        //fn has not yet been called
        val nextFWT = fn(t)
        // Send the timeout function through
        resref.set({ () => nextFWT.getNow })
        nextFWT.future
      }
      else {
        // someone called onTimeout
        Future.exception(new Exception("we timed out waiting"))
      }
    }
    // Don't capture any refs in here other than future, and one we will null
    // if Future leaks memory by holding refs, that's future's fault
    new FlatMappedFutureWithDefault[T,U](nextFuture, (this, fn, resref, fnref))
  }

  def map[U](fn: T => U): FutureWithDefault[U] =
    flatMap { (t: T) => FutureWithDefault.value(fn(t)) }

  // The result is considered to have timedout is either this or u did
  def join[U](u: FutureWithDefault[U]): FutureWithDefault[(T,U)] =
    FutureWithDefault.onTimeout(future.join(u.future)) {
      for(tot <- getNow; tou <- u.getNow) yield (tot, tou)
    }
  // If this doesn't return, call u.getNow
  def orGetNow[U >:T](u: FutureWithDefault[U]): FutureWithDefault[U] =
    FutureWithDefault.onTimeout[U](future)(u.getNow)
}

private class FlatMappedFutureWithDefault[T,U](override val future: Future[U],
  var state: (FutureWithDefault[T],
              T => FutureWithDefault[U],
              AtomicRef[() => Try[U]],
              AtomicRef[T => FutureWithDefault[U]]))
  extends FutureWithDefault[U](future) {

  // Make sure we eventually null out state:
  future.respond { tu: Try[U] =>
    //Realize the on timeout so it is safe to move with life and null out gc
    this.onTimeout
  }

  lazy val onTimeout = {
    val (prev, fn, resref, fnref) = state
    // Null out the state to avoid holding too long and preventing GC
    state = null
    if(fnref.compareAndSet(fn, null)) {
      timedoutRef.set(true)
      // No one has yet called fn, so we are safe to:
      prev.getNow.flatMap { t => fn(t).getNow }
    }
    else {
      // We have already called fn so we have setup a timeout value:
      var timeoutFn: Function0[Try[U]] = null
      //Spin because fn is not blocking, we should have the value:
      while(null == timeoutFn) { timeoutFn = resref.get() }
      timeoutFn()
    }
  }
}

