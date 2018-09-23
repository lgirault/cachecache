package cachecache

import cachecache.Cache.TimeSpec
import cats.Alternative
import cats.effect._
import cats.effect.concurrent.Ref
import cats.instances.option._
import cats.syntax.flatMap._
import cats.syntax.functor._

import scala.collection.immutable.Map
import scala.concurrent.duration._

object Cache {

  // Value of Time In Nanoseconds
  class TimeSpec private(
                          val nanos: Long
                        ) extends AnyVal

  object TimeSpec {

    def fromDuration(duration: FiniteDuration): Option[TimeSpec] =
      Alternative[Option].guard(duration > 0.nanos).as(unsafeFromDuration(duration))

    def unsafeFromDuration(duration: FiniteDuration): TimeSpec =
      new TimeSpec(duration.toNanos)

    def fromNanos(l: Long): Option[TimeSpec] =
      Alternative[Option].guard(l > 0).as(unsafeFromNanos(l))

    def unsafeFromNanos(l: Long): TimeSpec =
      new TimeSpec(l)

  }

  private sealed abstract class CacheContent[F[_], A]

  private case class Fetching[F[_], A](current: Fiber[F, A],
                                       autoReload: Fiber[F, Unit]) extends CacheContent[F, A] {

    def cancel(implicit M: Monad[F]): F[Unit] =
      current.cancel >> autoReload.cancel

  }


  private case class CacheItem[F[_], A](
                                         item: A,
                                         itemExpiration: Option[TimeSpec],
                                         autoReload : Option[Fetching[F, A]]
                                       ) extends CacheContent[F, A]

  /**
    * Create a new cache with a default expiration value for newly added cache items.
    *
    * Items that are added to the cache without an explicit expiration value (using insert) will be inserted with the default expiration value.
    *
    * If the specified default expiration value is None, items inserted by insert will never expire.
    **/
  def createCache[F[_] : Concurrent : Timer, K, V](defaultExpiration: Option[TimeSpec],
                                                   defaultReloadTime: Option[TimeSpec])
                                                  (fetch: K => F[V]): F[Cache[F, K, V]] =
    Ref.of[F, Map[K, CacheContent[F, V]]](Map.empty[K, CacheContent[F, V]])
      .map(new Cache[F, K, V](_, defaultExpiration, defaultReloadTime, fetch))


  /**
    * Return the size of the cache, including expired items.
    **/
  def size[F[_] : Sync, K, V](cache: Cache[F, K, V]): F[Int] =
    cache.ref.get.map(_.size)

  /**
    * Return all keys present in the cache, including expired items.
    **/
  def keys[F[_] : Sync, K, V](cache: Cache[F, K, V]): F[List[K]] =
    cache.ref.get.map(_.keys.toList)

  /**
    * Delete an item from the cache. Won't do anything if the item is not present.
    **/
  def delete[F[_] : Sync, K, V](cache: Cache[F, K, V])(k: K): F[Unit] =
    cache.ref.update(m => m - k).void


  /**
    * Insert an item in the cache, using the default expiration value of the cache.
    */
  def insert[F[_] : Sync : Timer, K, V](cache: Cache[F, K, V])(k: K, v: V): F[Unit] =
    insertWithTimeout(cache)(cache.defaultExpiration)(k, v)


  /**
    * Insert an item in the cache, with an explicit expiration value.
    *
    * If the expiration value is None, the item will never expire. The default expiration value of the cache is ignored.
    *
    * The expiration value is relative to the current clockMonotonic time, i.e. it will be automatically added to the result of clockMonotonic for the supplied unit.
    **/

  def insertWithTimeout[F[_] : Sync, K, V](cache: Cache[F, K, V])
                                          (optionTimeout: Option[TimeSpec])
                                          (k: K, v: V)
                                          (implicit T: Timer[F]): F[Unit] = {

    def updateMap(timeout: Option[TimeSpec])
                 (m: Map[K, CacheContent[F, V]]) : Map[K, CacheContent[F, V]] =
      m.get(k) match {
        case None => m + (k -> CacheItem[F, V](v, timeout, None))
        case Some(content : Fetching[F, V]) => m + (k -> CacheItem[F, V](v, timeout, Some(content)))
        case Some(content : CacheItem[F, V]) => m + (k -> CacheItem[F, V](v, timeout, content.autoReload))
      }

    for {
      now <- T.clock.monotonic(NANOSECONDS)
      timeout = optionTimeout.map(ts => TimeSpec.unsafeFromNanos(now + ts.nanos))
      _ <- cache.ref.update(updateMap(timeout))
    } yield ()
  }


  private def insertFetch[F[_], K, V](k: K, cache: Cache[F, K, V])
                                     (current: Fiber[F, V], f: Fiber[F, Unit])
                                     (implicit S: Sync[F]): F[Unit] = {

    val newBinding = (k ->  Fetching[F, V](current, f))

    def updateMap(m: Map[K, CacheContent[F, V]]) : (Map[K, CacheContent[F, V]], F[Unit]) = {
      (m + newBinding, m.get(k) match {
        case None =>   S.unit
        case Some(content : Fetching[F, V]) => content.cancel
        case Some(content : CacheItem[F, V]) => content.autoReload.map(_.cancel) getOrElse S.unit
      }

    }
    cache.ref.modify(updateMap).flatten
  }



  private def isExpired[F[_], A](checkAgainst: TimeSpec, cacheItem: CacheItem[F, A]): Boolean = {
    cacheItem.itemExpiration.fold(false) {
      case e if e.nanos < checkAgainst.nanos => true
      case _ => false
    }
  }

  private def lookupItemSimple[F[_] : Sync, K, V](k: K, c: Cache[F, K, V]): F[Option[CacheContent[F, V]]] =
    c.ref.get.map(_.get(k))

  private def fetchInsert[F[_] : Sync : Timer, K, V](k: K, c: Cache[F, K, V]): F[V] =
    for {
      v <- c.fetch(k)
      _ <- insert(c)(k, v)
    } yield v


  private def autoReload[F[_], K, V](k: K, c: Cache[F, K, V], t: TimeSpec)
                                    (implicit C: Concurrent[F],
                                     T: Timer[F]): F[Unit] =
    c.defaultReloadTime.map {
      reloadTime =>

        def loop(): F[Unit] = //XXX is it stack safe ??
          C.start[V](T.sleep(Duration.fromNanos(reloadTime.nanos)) >> c.fetch(k))
            .flatMap { fiber: Fiber[F, V] =>

              C.start[Unit](fiber.join.flatMap(insert(c)(k, _)) >> loop() )
                  .flatMap(insertFetch(k, c)(fiber, _))
            }

        loop()

    } getOrElse C.unit

  /**
    * Internal Function Used for Lookup and management of values.
    * If isExpired and The boolean for delete is present then we delete,
    * otherwise return the value.
    **/

  private def extractContentT[F[_] : Timer, K, V](k: K, c: Cache[F, K, V], t: TimeSpec)
                                                 (content: CacheContent[F, V])
                                                 (implicit S: Sync[F]): F[V] = content match {
    case v0: Fetching[F, V] => v0.current.join
    case v0: CacheItem[F, V] =>
      if (isExpired(t, v0)) fetchInsert(k, c)
      else S.pure(v0.item)
  }

  private def lookupItemT[F[_] : Timer, K, V](k: K, c: Cache[F, K, V], t: TimeSpec)
                                             (implicit C: Concurrent[F]): F[V] = {
    for {
      i <- lookupItemSimple(k, c)
      v <- i match {
        case None => //first lookup, launch autoreload if needed
          autoReload(k, c, t) >> fetchInsert(k, c)
        case Some(content) => extractContentT(k, c, t)(content)
      }
    } yield v
  }


  def lookup[F[_] : Concurrent, K, V](c: Cache[F, K, V])
                                     (k: K)
                                     (implicit T: Timer[F]): F[V] =
    T.clock.monotonic(NANOSECONDS)
      .flatMap(now => lookupItemT(k, c, TimeSpec.unsafeFromNanos(now)))

}

class Cache[F[_] : Concurrent : Timer, K, V](private val ref: Ref[F, Map[K, Cache.CacheContent[F, V]]],
                                             val defaultExpiration: Option[TimeSpec],
                                             val defaultReloadTime: Option[TimeSpec],
                                             val fetch: K => F[V]) {

  def lookup(k: K): F[V] = Cache.lookup(this)(k)
}


object Main extends IOApp {

  def run(args: List[String]): IO[ExitCode] = {


    val cc = Cache.createCache[IO, String, String](None, None) { s: String =>
      IO.delay {
        println(s"working on $s")
        s.toUpperCase
      }
    }

    for {
      c <- cc
      v <- c.lookup("toto")
      v1 <- c.lookup("toto")
      v2 <- c.lookup("toto")

      _ = println((v, v1, v2))
    } yield ExitCode.Success



    //println(program.unsafeRunSync())
  }

}
