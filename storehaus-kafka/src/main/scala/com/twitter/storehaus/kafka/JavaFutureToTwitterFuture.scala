/*
 * Copyright 2014 Twitter inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.twitter.storehaus.kafka

import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.{Future => JFuture}

import com.twitter.util.{Future, Try, Promise}

import scala.annotation.tailrec

/** Utility class for converting Java futures to Twitter's */
private[kafka] class JavaFutureToTwitterFuture {
  
  def apply[T](javaFuture: JFuture[T]): Future[T] = {
    val promise = new Promise[T]()
    poll(Link(javaFuture, promise))
    promise
  }
  
  private val WAIT_TIME_MS = 1000
  private val pollRun = new Runnable {
    override def run(): Unit = loop(list.getAndSet(Nil))
    
    @tailrec
    def loop(links: List[Link[_]]): Unit = {
      val notDone = links.filterNot(_.maybeUpdate)
      if (links.isEmpty || notDone.nonEmpty) Thread.sleep(WAIT_TIME_MS)
      loop(list.getAndSet(notDone))
    }
  }
  private val list = new AtomicReference[List[Link[_]]](Nil)
  private val thread = new Thread(pollRun)
  
  def start(): Unit = {
    thread.setDaemon(true)
    thread.start()
  }
  
  private def poll[T](link: Link[T]): Unit = {
    val tail = list.get()
    if (list.compareAndSet(tail, link :: tail)) ()
    else poll(link)
  }
  
  case class Link[T](future: JFuture[T], p: Promise[T]) {
    def maybeUpdate: Boolean = future.isDone && {
      p.update(Try(future.get()))
      true
    }
  }
}
