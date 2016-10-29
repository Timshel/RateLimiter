package timshel

import akka.stream.{ActorMaterializer}
import akka.stream.scaladsl.{Keep, Sink}
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration.FiniteDuration

case class AsyncCall[T](wrapped: () => Future[T]) extends Call {
  val promise = Promise[T]

  def run: Future[T] = {
    val call = wrapped()
    promise.completeWith(call)
    call
  }

  def dropped() = {
    promise.failure(new java.util.concurrent.TimeoutException("Call was dropped"))
    ()
  }

}

class RateLimiter(
  val limit:        Int,
  val time:         FiniteDuration,
  val parallelism:  Int,
  val bufferSize:   Int
)(
  implicit
  materializer: ActorMaterializer
) {

  val (input, out) =  ActorRefSource[AsyncCall[_]](bufferSize)
                          .via(new SlidingThrottle(limit, time))
                          .mapAsync(parallelism){ call => call.run }
                          .toMat(Sink.ignore)(Keep.both)
                          .run()

  def enqueue[T](call: => Future[T]): Future[T] = {
    val ac = AsyncCall(() => call)
    input ! ac
    ac.promise.future
  }
}

class RateLimiterWithTimeout(
  limit:        Int,
  time:         FiniteDuration,
  timeout:      FiniteDuration,
  parallelism:  Int
)(
  implicit
  bufferSize:   Int = RateLimiterWithTimeout.bufferSize(limit, time, timeout),
  ex:           ExecutionContext,
  system:       akka.actor.ActorSystem,
  materializer: ActorMaterializer
) extends RateLimiter(limit, time, parallelism, bufferSize)(materializer) {

  def withTimeout[T](fut: Future[T], delay: FiniteDuration): Future[T] = {
    val delayed = akka.pattern.after(delay, using = system.scheduler)(Future.failed[T](
      new java.util.concurrent.TimeoutException("Timeout"))
    )
    Future.firstCompletedOf(Seq(fut, delayed))
  }

  override def enqueue[T](call: => Future[T]): Future[T] = {
    withTimeout(super.enqueue(call), timeout)
  }

}


object RateLimiterWithTimeout {

  /**
   * The size is equal to 90% of the capacity than can be handled during the timeout period.
   * We drop the oldest element when overflowing
   * It cannot be lower than the limit.
   * Ex : limit 100, time: 1.s timeout 1.min => bufferSize = 5400
   */
  def bufferSize(limit: Int, time: FiniteDuration, timeout: FiniteDuration): Int =
    math.max(limit, ((timeout / time) * limit * 0.9d).toInt)

}
