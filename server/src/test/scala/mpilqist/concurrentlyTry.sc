import cats.effect.IO, cats.effect.unsafe.implicits.global
import fs2.Stream, fs2.concurrent.SignallingRef

val data: Stream[IO,Int] = Stream.range(1, 10).covary[IO]

Stream.eval(SignallingRef[IO,Int](0))
.flatMap: s => 
    Stream(s).concurrently(data.evalMap(s.set))
.flatMap:
  _.discrete
.takeWhile(_ < 9, true)
  .compile.last.unsafeRunSync()
