package com.twitter.storehaus.redis

import com.twitter.bijection.Bijection
import com.twitter.finagle.redis.util.{StringToBuf,BufToString}
import com.twitter.io.Buf
import com.twitter.storehaus.algebra.MergeableStore
import com.twitter.storehaus.testing.CloseableCleanup
import com.twitter.util.{Await, Future}
import org.scalatest.{Matchers, WordSpec}

class RedisSortedSetSpec extends WordSpec with Matchers
  with CloseableCleanup[RedisSortedSetStore]
  with DefaultRedisClient {
  import com.twitter.bijection.Bijection._

  implicit def strToCb: Bijection[String, Buf] =
    Bijection.build(StringToBuf(_: String))(
      BufToString(_: Buf))

  val closeable: RedisSortedSetStore =
    RedisSortedSetStore(client)

  val sets: MergeableStore[String, Seq[(String, Double)]] =
    closeable.convert(StringToBuf(_: String))

  val members: MergeableStore[(String, String), Double] =
    closeable.members.convert {
      case (s, m) => (StringToBuf(s), StringToBuf(m))
    }

  val commits = Seq(("sritchie", 137.0), ("softprops", 73.0),
                    ("rubanm", 32.0), ("johnynek", 17.0))

  object ::> {
    def unapply(xs: scala.collection.TraversableLike[_, _]): Option[(_, _)] =
      if (xs.isEmpty) None else Some((xs.init, xs.last))
  }

  "RedisSortedSet" should {
    "support Store operations" in {
      Await.result(for {
        put <- sets.put(("commits", Some(commits)))
        commits <- sets.get("commits")
      } yield commits) should be(Some(commits.sortWith(_._2 < _._2)))
    }

    "support merge operations" in {
      val merged = Await.result(for {
        _ <- sets.merge(("commits", Seq(("sritchie", 1.0))))
        commits <- sets.get("commits")
      } yield commits)
      (for (_ ::> last <- merged) yield last) should be(Some(
        ("sritchie", 138.0)))
    }
    "support delete operation" in {
      Await.result(for {
        _ <- sets.put(("commits", None))
        commits <- sets.get("commits")
      } yield commits) should be(None)
    }
  }

  "RedisSortedSet#members" should {

    val putting = commits.map { case (m, d) => (("commits", m), Some(d)) }.toMap
    "support Store operations" in {
      // TODO(doug) Future.collect should really work with all iterables
      Await.result(Future.collect(members.multiPut(putting).values.toSeq))
      putting.foreach {
        case (k, v) =>
          Await.result(members.get(k)) should equal(v)
      }
    }
    "support merge operations" in {
      val who = ("commits", "sritchie")
      Await.result(for {
        _ <- members.merge((who, 1.0))
        score <- members.get(who)
      } yield score) should be(Some(138.0))
    }
    "support delete operation" in {
      val who = ("commits", "sritchie")
      Await.result(for {
        _ <- members.put((who, None))
        score <- members.get(who)
      } yield score) should be(None)
    }
  }
}
