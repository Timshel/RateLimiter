package timshel

import akka.stream.impl.fusing.GraphStages.SimpleLinearGraphStage
import akka.stream.stage._
import akka.stream._

import scala.concurrent.duration.{ FiniteDuration, _ }

/**
 * Implementation of a Throttle with a sliding window
 * Inspired by ()
 */
class SlidingThrottle[T](max: Int, per: FiniteDuration) extends SimpleLinearGraphStage[T] {
  require(max > 0, "max must be > 0")
  require(per.toNanos > 0, "per time must be > 0")
  require(per.toNanos >= max, "Rates larger than 1 unit / nanosecond are not supported")

  private val nanosPer = per.toNanos
  private val timerName: String = "ThrottleTimer"

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new TimerGraphStageLogic(shape) {
    var willStop = false

    var emittedTimes = scala.collection.immutable.Queue.empty[Long]
    var last: Long = System.nanoTime
    var currentElement: T = _

    def pushThenLog(elem: T): Unit = {
      push(out, elem)
      last = System.nanoTime
      emittedTimes = emittedTimes :+ last
      if( willStop ) completeStage()
    }

    def schedule(elem: T, nanos: Long): Unit = {
      currentElement = elem
      scheduleOnce(timerName, nanos.nanos)
    }

    def receive(elem: T): Unit = {
      var now = System.nanoTime
      emittedTimes = emittedTimes.dropWhile { t => t + nanosPer < now }

      if( emittedTimes.length < max ) pushThenLog(elem)
      else schedule(elem, emittedTimes.head + nanosPer - System.nanoTime)
    }

    // This scope is here just to not retain an extra reference to the handler below.
    // We can't put this code into preRestart() because setHandler() must be called before that.
    {
      val handler = new InHandler with OutHandler {
        override def onUpstreamFinish(): Unit =
          if (isAvailable(out) && isTimerActive(timerName)) willStop = true
          else completeStage()

        override def onPush(): Unit = receive(grab(in))

        override def onPull(): Unit = pull(in)
      }

      setHandler(in, handler)
      setHandler(out, handler)
      // After this point, we no longer need the `handler` so it can just fall out of scope.
    }

    override protected def onTimer(key: Any): Unit = {
      var elem = currentElement
      currentElement = null.asInstanceOf[T]
      receive(elem)
    }

  }

  override def toString = "Throttle"
}
