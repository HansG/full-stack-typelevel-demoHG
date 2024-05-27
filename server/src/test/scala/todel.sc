import cats.effect.{SyncIO,IO}
import fs2.*
import mpilquist.Fs2WalRe.*
import cats.effect.unsafe.implicits.global

val strm = Stream(1, 2, 3) ++ Stream.raiseError[SyncIO](new RuntimeException) ++ Stream(4, 5, 6)
strm.mask.compile.toList.unsafeRunSync()

val ret =
  for
    ld <- makeLargeDir
    count <- walkSimple[IO](ld).compile.count
  yield (ld, count)

val timeR = time(ret.unsafeRunSync())
println(timeR)

println(time(java.nio.file.Files.walk( timeR._2._1  .toNioPath).count()))