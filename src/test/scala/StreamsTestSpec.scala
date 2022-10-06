import akka.{Done, NotUsed}
import akka.actor.{ActorSystem, Cancellable}
import akka.stream.FlowShape
import akka.stream.scaladsl._
import akka.testkit.TestKit
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.DurationInt
import scala.language.postfixOps

class StreamsTestSpec extends TestKit(ActorSystem("Dagobbah"))
  with AnyWordSpecLike
  with Matchers
  with BeforeAndAfterAll {

  def expensiveOperation(item: Int) = {

    Thread.sleep(1000)
    item * 2
  }

  def expensiveOperation2(pair: (Int, Long)) = {

    Thread.sleep(1000)
    pair._1 * 2 -> pair._2
  }


  override def afterAll(): Unit = {
    super.afterAll()
    TestKit.shutdownActorSystem(system, 5.minutes, verifySystemShutdown = true)
  }

  implicit val ex = system.dispatcher
  val parallelItems = 5

  val ackExtender = new AckExtenderImpl
  "wireTapMat within a flatmapMerge " should {
    "behave like this" in {
      Source(1 to 5)
        .alsoTo(Sink.foreach { x =>
          Thread.sleep(100)
          println(s"elem:$x alsoTo")
        }
        )
        .map { x =>
          println(s"elem:$x map")
          x
        }
        .to(Sink.foreach(x => println(s"elem:$x to")))
        .run()
    }

    "not behave like this" in {
      println("Hello world")
      // Observe the elapsed time for extending item msgs!
      val testStart = System.currentTimeMillis()
      val eventualDone: Future[Done] =
        Source(1 to 5)
          .flatMapMerge(parallelItems, item => {
            Source.single {
              item
            }.wireTapMat(Flow[Int].map[(String, Long)]((_.toString -> testStart)).toMat(ackExtender.sink)(Keep.right))(Keep.right)
              .map(expensiveOperation)
          })
          .runWith(Sink.foreach(v => println(s"Processed item $v at ${System.currentTimeMillis() - testStart}")))

      Await.result(eventualDone, 5.minutes)
    }

    val sideSink: Sink[(Int, Long), Cancellable] = new AckExtenderImpl2().sink

    val bc: Flow[(Int, Long), (Int, Long), NotUsed] = Flow.fromGraph(GraphDSL.create() {
      implicit builder => {
        import GraphDSL.Implicits._

        val broadcast = builder.add(Broadcast[(Int, Long)](2))
        val expensiveOp = builder.add(Flow[(Int, Long)].map[(Int, Long)](expensiveOperation2))
        val ackExtender = builder.add(sideSink)
        broadcast.out(0) ~> ackExtender
        broadcast.out(1) ~> expensiveOp
        FlowShape(broadcast.in, expensiveOp.out)
      }
    })

    "but could be made to work like that" in {
      // Observe the elapsed time for extending item msgs!
      val testStart = System.currentTimeMillis()
      val eventualDone: Future[Done] =
        Source(1 to 5)
          .flatMapMerge(parallelItems, item => {
            Source.single {
              item -> testStart
            }.via(bc)
              .map(expensiveOperation2)
          })
          .runWith(Sink.foreach(v => println(s"Processed item $v at ${System.currentTimeMillis() - testStart}")))

      Await.result(eventualDone, 5.minutes)
    }
  }
}
