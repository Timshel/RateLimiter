import akka.stream.{ActorMaterializer, OverflowStrategy}
import akka.stream.scaladsl.{Keep, Sink, Source}
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration.FiniteDuration

class RateLimiter(
  val limit:        Int,
  val time:         FiniteDuration,
  val parallelism:  Int,
  val bufferSize:   Int
)(
  implicit
  materializer: ActorMaterializer
) {
  val (input, out) =  Source.actorRef[() => Future[_]](bufferSize, OverflowStrategy.dropHead)
                          .via(new SlidingThrottle(limit, time))
                          .mapAsync(parallelism){ call => call() }
                          .toMat(Sink.ignore)(Keep.both)
                          .run()

  def enqueue[T](call: => Future[T]): Future[T] = {
    val promise = Promise[T]
    val wrapped = { () => promise.completeWith(call) }

    input ! wrapped

    promise.future
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
    val delayed = akka.pattern.after(delay, using = system.scheduler)(Future.failed[T](new RuntimeException("Timeout")))
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
