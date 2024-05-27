package mpilqist
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import fs2.Stream
import fs2.concurrent.{Channel, SignallingRef}
import munit.{CatsEffectSuite, ScalaCheckEffectSuite}

class ConcurrentlyTry extends CatsEffectSuite with ScalaCheckEffectSuite:


  val data: Stream[IO, Int] = Stream.range(1, 10).covary[IO]

  test("SignallingRef") {
    Stream.eval(SignallingRef[IO, Int](0))
      .flatMap: s =>
        Stream(s).concurrently(data.evalTap(n => IO(println(n))).evalMap(s.set))
      .flatMap:
        _.discrete
      .takeWhile(_ < 9, true)
      .evalTap(n => IO(println("Main: "+n)))
      .compile.last//
    // .unsafeRunSync()
  }

  test("Channel") {
    Stream.eval(Channel.synchronous[IO, Int])
      .flatMap: s =>
        s.stream.concurrently(data.evalTap(n => IO(println(n))).evalMap(s.send(_)))
      .takeWhile(_ < 5, true)
      .evalTap(n => IO(println("Main: "+n)))
      .compile.last//
    // .unsafeRunSync()
  }


