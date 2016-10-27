
import akka.stream.impl.Stages.DefaultAttributes
import akka.actor.ActorRef
import akka.stream.OverflowStrategy

object RLSource {
  import akka.stream.scaladsl.Source, Source._

  def actorRef[T](bufferSize: Int, overflowStrategy: OverflowStrategy)(system: akka.actor.ActorSystem): Source[T, ActorRef] = {
    require(bufferSize >= 0, "bufferSize must be greater than or equal to 0")
    new Source(new ActorRefSource(
      bufferSize,
      overflowStrategy,
      DefaultAttributes.actorRefSource,
      shape("ActorRefSource"),
      system
    ))
  }
}

import akka.actor._
import akka.stream._
import akka.stream.impl.StreamLayout.AtomicModule
import akka.stream.impl.SourceModule

final class ActorRefSource[Out](
  bufferSize: Int,
  overflowStrategy: OverflowStrategy,
  val attributes: Attributes,
  shape: SourceShape[Out],
  system: akka.actor.ActorSystem
) extends SourceModule[Out, ActorRef](shape) {

  override protected def label: String = s"ActorRefSource($bufferSize, $overflowStrategy)"

  override def create(context: MaterializationContext) = {
    val ref = system.actorOf(ActorRefSourceActor.props(bufferSize, overflowStrategy), label)
    (akka.stream.actor.ActorPublisher[Out](ref), ref)
  }

  override protected def newInstance(shape: SourceShape[Out]): SourceModule[Out, ActorRef] =
    new ActorRefSource[Out](bufferSize, overflowStrategy, attributes, shape, system)

  override def withAttributes(attr: Attributes): AtomicModule =
    new ActorRefSource(bufferSize, overflowStrategy, attr, amendShape(attr), system)
}

import akka.actor.ActorLogging
import akka.actor.Props
import akka.actor.Status
import akka.stream.OverflowStrategy

private object ActorRefSourceActor {
  def props(bufferSize: Int, overflowStrategy: OverflowStrategy) = {
    Props(new ActorRefSourceActor(bufferSize, overflowStrategy))
  }
}

private class ActorRefSourceActor(bufferSize: Int, overflowStrategy: OverflowStrategy)
  extends akka.stream.actor.ActorPublisher[Any] with ActorLogging {
  import akka.stream.actor.ActorPublisherMessage._

  // when bufferSize is 0 there the buffer is not used
  protected val buffer = scala.collection.mutable.Queue[Any]()

  //if (bufferSize == 0) null else Buffer[Any](bufferSize, maxFixedBufferSize)

  def receive = ({
    case Cancel ⇒
      context.stop(self)

    case _: Status.Success ⇒
      if (bufferSize == 0 || buffer.isEmpty) context.stop(self) // will complete the stream successfully
      else context.become(drainBufferThenComplete)

    case Status.Failure(cause) if isActive ⇒
      onErrorThenStop(cause)

  }: Receive).orElse(requestElem).orElse(receiveElem)

  def requestElem: Receive = {
    case _: Request ⇒
      // totalDemand is tracked by super
      if (bufferSize != 0)
        while (totalDemand > 0L && !buffer.isEmpty)
          onNext(buffer.dequeue())
  }

  def receiveElem: Receive = {
    case elem if isActive ⇒
      if (totalDemand > 0L)
        onNext(elem)
      else if (buffer.size < bufferSize )
        buffer.enqueue(elem)
      else {
        log.debug("Dropping the head element because buffer is full and overflowStrategy is: [DropHead]")
        buffer.dequeue()
        buffer.enqueue(elem)
      }
  }

  def drainBufferThenComplete: Receive = {
    case Cancel ⇒
      context.stop(self)

    case Status.Failure(cause) if isActive ⇒
      // errors must be signaled as soon as possible,
      // even if previously valid completion was requested via Status.Success
      onErrorThenStop(cause)

    case _: Request ⇒
      // totalDemand is tracked by super
      while (totalDemand > 0L && !buffer.isEmpty)
        onNext(buffer.dequeue())

      if (buffer.isEmpty) context.stop(self) // will complete the stream successfully

    case elem if isActive ⇒
      log.debug("Dropping element because Status.Success received already, " +
        "only draining already buffered elements: [{}] (pending: [{}])", elem, buffer.size)
  }

}
