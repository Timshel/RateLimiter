import java.util.concurrent.TimeoutException

import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.SpanSugar._
import scala.concurrent.{Await, Future}

class RateLimiterSpec extends WordSpec with Matchers with ScalaFutures with BeforeAndAfterAll{
  import scala.concurrent.ExecutionContext.Implicits.global

  implicit val system = akka.actor.ActorSystem()
  implicit val materializer = akka.stream.ActorMaterializer()

  def fd(span: org.scalatest.time.Span): scala.concurrent.duration.FiniteDuration = {
    new scala.concurrent.duration.FiniteDuration(span.length, span.unit)
  }

  "RateLimiter" should {
    "enqueue" in {
      val queue = new RateLimiter(10, fd(1.second), 20, 100)
      val res = queue.enqueue(Future(12))

      assert(res.isReadyWithin(200.millis))
      assert(res.futureValue === 12)
    }

    "limit" in {
      val queue = new RateLimiter(1, fd(2.seconds), 2, 100)

      (0 until 1).foreach { _ => queue.enqueue(Future { 1 }) }
      val res = queue.enqueue(Future { 2 })

      val _ = intercept[TimeoutException] { Await.result(res, 1.second) }

      assert(res.isReadyWithin(2.seconds))
      assert(res.futureValue === 2)
    }

    "limit burst" in {
      val queue = new RateLimiter(10, fd(2.seconds), 20, 100)
      val start = System.currentTimeMillis
      val delay = 20

      val values = Future.sequence(
        (0 until 3 * queue.limit).map { i => queue.enqueue(Future { i -> (System.currentTimeMillis - start) }) }
      )

      assert(values.isReadyWithin(queue.time * 3))

      values.futureValue.sortBy(_._1).grouped(queue.limit).zipWithIndex.foreach { case (grouped, index) =>
        assert(grouped.head._2 < index * queue.time.toMillis + (index + 1) * delay)
        grouped.map(_._2).sliding(2, 1).map {
          case Seq(a, b) => assert(b - a < delay)
        }
      }
    }

    "limit parallelism" in {
      val queue = new RateLimiter(10, fd(3.seconds), 5, 100)
      val start = System.currentTimeMillis
      val sleep = 1.second
      val delay = 20

      val values = Future.sequence(
        (0 until 3 * queue.limit).map { i => queue.enqueue(Future {
          Thread.sleep(sleep.toMillis)
          i -> (System.currentTimeMillis - start)
        }) }
      )

      assert(values.isReadyWithin(queue.time * 3))

      values.futureValue.sortBy(_._1).grouped(queue.limit).zipWithIndex.foreach { case (groupedL, indexL) =>
        groupedL.grouped(queue.parallelism).zipWithIndex.foreach { case (groupedP, indexP) =>
          val limit = indexL * queue.time.toMillis + (indexP + 1) * sleep.toMillis + (indexL + indexP + 1) * delay
          assert(groupedP.head._2 < limit)
          groupedP.map(_._2).sliding(2, 1).map {
            case Seq(a, b) => assert(b - a < delay)
          }
        }
      }
    }
  }

  "RateLimiterWithTimeout" should {
    "timeout" in {
      val queue = new RateLimiterWithTimeout(10, fd(1.minute), fd(1.second), 100)
      val res = queue.enqueue(Future { Thread.sleep(1.minute.toMillis) })

      val _ = intercept[TimeoutException] {
        Await.result(res, 1.hour)
      }
    }
  }

  override def afterAll = {
    materializer.shutdown
    system.terminate().futureValue
    ()
  }
}
