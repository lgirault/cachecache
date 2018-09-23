package cachecache

import cachecache.Cache.TimeSpec
import cats.Alternative
import cats.effect._
import cats.effect.concurrent.Ref
import cats.instances.option._
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.syntax.traverse._

import scala.collection.immutable.Map
import scala.concurrent.duration._

class Cache[F[_] : Concurrent : Timer, K, V](private val values: Ref[F, Map[K, Cache.CacheContent[F, V]]],
                                             val defaultExpiration: Option[TimeSpec],
                                             private val reload: Option[(TimeSpec, Ref[F, Map[K, Fiber[F, Unit]]])],
                                             val fetch: K => F[V]) {

  def lookup(k: K): F[V] = Cache.lookup(this)(k)
}

object Cache {

  private sealed abstract class CacheContent[F[_], A]

  private case class Fetching[F[_], A](f: Fiber[F, A]) extends CacheContent[F, A]


  private case class CacheItem[F[_], A](
                                         item: A,
                                         itemExpiration: Option[TimeSpec]
                                       ) extends CacheContent[F, A]


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
    for {
      valuesRef <- Ref.of[F, Map[K, CacheContent[F, V]]](Map.empty)
      reloads <- defaultReloadTime.traverse(timeSpec =>
        Ref.of[F, Map[K, Fiber[F, Unit]]](Map.empty).map((timeSpec, _))
      )
    } yield new Cache[F, K, V](valuesRef, defaultExpiration, reloads, fetch)


  /**
    * Return the size of the cache, including expired items.
    **/
  def size[F[_] : Sync, K, V](cache: Cache[F, K, V]): F[Int] =
    cache.values.get.map(_.size)

  /**
    * Return all keys present in the cache, including expired items.
    **/
  def keys[F[_] : Sync, K, V](cache: Cache[F, K, V]): F[List[K]] =
    cache.values.get.map(_.keys.toList)

  /**
    * Delete an item from the cache. Won't do anything if the item is not present.
    **/
  def delete[F[_] : Sync, K, V](cache: Cache[F, K, V])(k: K): F[Unit] =
    cache.values.update(m => m - k).void


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
    for {
      now <- T.clock.monotonic(NANOSECONDS)
      timeout = optionTimeout.map(ts => TimeSpec.unsafeFromNanos(now + ts.nanos))
      _ <- cache.values.update(_ + (k -> CacheItem[F, V](v, timeout)))
    } yield ()
  }


  private def insertFetching[F[_], K, V](k: K, cache: Cache[F, K, V])
                                        (f: Fiber[F, V])
                                        (implicit S: Sync[F]): F[Unit] = {
    cache.values.update(_ + (k -> Fetching[F, V](f)))
  }


  private def isExpired[F[_], A](checkAgainst: TimeSpec, cacheItem: CacheItem[F, A]): Boolean = {
    cacheItem.itemExpiration.fold(false) {
      case e if e.nanos < checkAgainst.nanos => true
      case _ => false
    }
  }

  private def lookupItemSimple[F[_] : Sync, K, V](k: K, c: Cache[F, K, V]): F[Option[CacheContent[F, V]]] =
    c.values.get.map(_.get(k))

  private def fetchAndInsert[F[_] : Sync : Timer, K, V](k: K, c: Cache[F, K, V]): F[V] =
    for {
      v <- c.fetch(k)
      _ <- insert(c)(k, v)
    } yield v


  /// XXX get and set is not atomic
  private def autoReload[F[_], K, V](k: K, c: Cache[F, K, V], t: TimeSpec)
                                    (implicit C: Concurrent[F],
                                     T: Timer[F]): F[Unit] =
    c.reload.map {
      case (tspec, reloads) =>

        def loop(): F[Unit] =
          C.start[V](T.sleep(Duration.fromNanos(tspec.nanos)) >> c.fetch(k))
            .flatMap { fiber =>
              insertFetching(k, c)(fiber) >> fiber.join >>= (insert(c)(k, _))
            } >> loop()


        val go: F[Unit] = reloads.get.map(_.contains(k)) >>= { alreadySetup =>
            if (alreadySetup) C.unit
            else C.start(loop()).map(f => reloads.update(_ + (k -> f)))
          }

        go
    } getOrElse C.unit

  private def extractContentT[F[_] : Timer, K, V](k: K, c: Cache[F, K, V], t: TimeSpec)
                                                 (content: CacheContent[F, V])
                                                 (implicit S: Sync[F]): F[V] = content match {
    case v0: Fetching[F, V] => v0.f.join
    case v0: CacheItem[F, V] =>
      if (isExpired(t, v0)) fetchAndInsert(k, c)
      else S.pure(v0.item)
  }

  private def lookupItemT[F[_] : Timer, K, V](k: K, c: Cache[F, K, V], t: TimeSpec)
                                             (implicit C: Concurrent[F]): F[V] = {
    for {
      _ <- autoReload(k, c, t)
      i <- lookupItemSimple(k, c)
      v <- i match {
        case None => fetchAndInsert(k, c)
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
