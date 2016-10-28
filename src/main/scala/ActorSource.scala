
import akka.actor.ActorRef
import akka.stream.{Attributes, MaterializationContext, SourceShape}
import akka.stream.impl.Stages.DefaultAttributes
import akka.stream.impl.StreamLayout.AtomicModule
import akka.stream.impl.SourceModule
import akka.stream.scaladsl.Source

trait Call {
  def dropped(): Unit
}

class ActorRefSource[T <: Call](
  bufferSize:       Int,
  val attributes:   Attributes = DefaultAttributes.actorRefSource,
  shape:            SourceShape[T] = Source.shape("ActorRefSource")
) extends SourceModule[T, ActorRef](shape) {

  override protected def label: String = s"ActorRefSource&bufferSize=$bufferSize"

  override def create(context: MaterializationContext) = {
    context.materializer match {
      case mat: akka.stream.ActorMaterializer =>
        val props = akka.actor.Props(new ActorRefSourceActor(bufferSize))
        val ref = mat.system.actorOf(props)
        (akka.stream.actor.ActorPublisher[T](ref), ref)
    }
  }

  override protected def newInstance(shape: SourceShape[T]): SourceModule[T, ActorRef] =
    new ActorRefSource(bufferSize, attributes, shape)

  override def withAttributes(attr: Attributes): AtomicModule =
    new ActorRefSource(bufferSize, attr, amendShape(attr))
}

object ActorRefSource {
  def apply[T <: Call](bufferSize: Int): Source[T, ActorRef] = {
    new Source(new ActorRefSource[T](bufferSize))
  }
}


class ActorRefSourceActor[T <: Call](
  bufferSize: Int
) extends akka.stream.actor.ActorPublisher[Any] with akka.actor.ActorLogging {

  import akka.stream.actor.ActorPublisherMessage._
  import akka.actor.Status

  protected val buffer = scala.collection.mutable.Queue[Call]()

  /**
   * totalDemand is tracked by super
   */
  def receive: Receive = {
    case Cancel => context.stop(self)
    case _: Status.Success =>
      if (bufferSize == 0 || buffer.isEmpty) context.stop(self) // will complete the stream successfully
      else context.become(drainBufferThenComplete)

    case Status.Failure(cause) if isActive ⇒
      onErrorThenStop(cause)
    case _: Request ⇒
      if (bufferSize != 0)
        while( totalDemand > 0L && !buffer.isEmpty )
          onNext(buffer.dequeue())
    case elem: Call if isActive ⇒
      if( totalDemand > 0L )
        onNext(elem)
      else if (buffer.size < bufferSize )
        buffer.enqueue(elem)
      else {
        log.debug("Dropping the head element because buffer is full and overflowStrategy is: [DropHead]")
        buffer.dequeue().dropped()
        buffer.enqueue(elem)
      }
  }

  def drainBufferThenComplete: Receive = {
    case Cancel => context.stop(self)

    case Status.Failure(cause) if isActive =>
      // errors must be signaled as soon as possible,
      // even if previously valid completion was requested via Status.Success
      onErrorThenStop(cause)

    case _: Request =>
      // totalDemand is tracked by super
      while (totalDemand > 0L && !buffer.isEmpty)
        onNext(buffer.dequeue())

      if (buffer.isEmpty) context.stop(self) // will complete the stream successfully

    case elem if isActive =>
      log.warning("Dropping element because Status.Success received already, " +
        "only draining already buffered elements: [{}] (pending: [{}])", elem, buffer.size)
  }

}
