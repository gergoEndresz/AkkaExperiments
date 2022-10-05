import akka.{Done, NotUsed}
import akka.actor.{ActorSystem, Cancellable}
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.{Duration, DurationInt, FiniteDuration}
import cats.syntax.either._

import scala.language.postfixOps

class AckExtenderImpl extends AckExtender[(String, Long), (String, Long)] {
   override def requestInterval: FiniteDuration = 1 seconds

   override def extendAckF(implicit as: ActorSystem, ec: ExecutionContext): ((String, Long)) => Future[Done] =
     (p) => {
       println(s"Extending item ${p._1} after ${System.currentTimeMillis()-p._2}!")
       Future.successful(Done)
     }


   override def feedName: String = "test"

   override def extendParallelism: Int = 1

  override def extractF: ((String, Long)) => (String, Long) = p => p

  override def accumulateF: ((String, Long), (String, Long)) => (String, Long) = (a, b) => b
}

class AckExtenderImpl2 extends AckExtender[(Int, Long), (String, Long)] {
  override def requestInterval: FiniteDuration = 1 seconds

  override def extendAckF(implicit as: ActorSystem, ec: ExecutionContext): ((String, Long)) => Future[Done] =
    (p) => {
      println(s"Extending item ${p._1} after ${p._2} millis!")
      Future.successful(Done)
    }


  override def feedName: String = "test"

  override def extendParallelism: Int = 1

  override def extractF: ((Int, Long)) => (String, Long) = p => (p._1.toString, System.currentTimeMillis()-p._2)

  override def accumulateF: ((String, Long), (String, Long)) => (String, Long) = (_, b) => b
}


trait AckExtender[In, Ack]  {
  def requestInterval: FiniteDuration
  def extendAckF(implicit as: ActorSystem, ec: ExecutionContext): Ack => Future[Done]
  def extractF: In => Ack
  def accumulateF: (Ack, Ack) => Ack
  def feedName: String
  def extendParallelism: Int

  def sink(implicit as: ActorSystem, ec: ExecutionContext): Sink[In, Cancellable] = {
    val ticks = Source.tick(Duration.Zero, requestInterval, NotUsed).map(_.asLeft)
    val doExtend = extendAckF
    Flow[In]
      .map(extractF)
      .map(_.asRight)
      .mergePreferredMat(ticks, preferred = true, eagerClose = false)(Keep.right)
      .statefulMapConcat { () =>
        var latest: Option[Ack] = None

        incoming =>
          (incoming, latest) match {
            case (Left(_), _) =>
              latest.toList
            case (Right(ack), Some(prev)) =>
              latest = Some(accumulateF(prev, ack))
              Nil
            case (Right(ack), None) =>
              latest = Some(ack)
              Nil
          }
      }
      .mapAsyncUnordered(extendParallelism)(doExtend)
      .toMat(Sink.ignore)(Keep.left)
  }
}

object AckExtender {

  implicit class AckExtenderSourceOps[Out](source: Source[Out, Cancellable]) {

    def stopExtenderWhenComplete(implicit ec: ExecutionContext): Source[Out, Future[Done]] =
      source
        .watchTermination() { (extender, completeF) =>
          completeF.onComplete(_ => extender.cancel())
          completeF
        }
  }

  implicit class AckExtenderFlowOps[In, Out](flow: Flow[In, Out, Cancellable]) {

    def stopExtenderWhenComplete(implicit ec: ExecutionContext): Flow[In, Out, Future[Done]] =
      flow
        .watchTermination() { (extender, completeF) =>
          completeF.onComplete(_ => extender.cancel())
          completeF
        }
  }
}