package timshel

import scala.concurrent.Future
import com.mfglabs.precepte._, default._

trait PreCapable {

  def enqueue[T](call: => Future[T]): Future[T]

  def enqueue[A, C](pre: DefaultPre[Future, C, A]): DefaultPre[Future, C, A] = {
    type SF[T] = (ST[C], Future[T])

    pre.mapSuspension {
      new (SF ~~> Future) {
        def apply[T](sf: SF[T]): Future[T] = enqueue(sf._2)
      }
    }
  }
}
