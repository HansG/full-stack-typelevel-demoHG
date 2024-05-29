package mpilquist

import scala.jdk.CollectionConverters.*

import cats.effect.{IO, Sync, Resource}
import cats.effect.unsafe.implicits.global
import cats.syntax.all.*
import fs2.Stream
import fs2.io.file.{Files, Path}
import munit.{CatsEffectSuite, ScalaCheckEffectSuite}

import scala.concurrent.duration.*

//https://github.com/mpilquist/blog-walks
/*§§
fs2.io.file.Files read, walk, fileAttributes
Stream zu IO : compile UND DANN toList/Vector foldMonid, count etc.
 */
class Fs2IoTry:

   def lineCount(p: Path): IO[Long] =
      Files[IO].readUtf8Lines(p).mask.compile.count

   def countFiles(dir: Path): IO[Long] =
      Files[IO].walk(dir).compile.count

   def totalSize(dir: Path): IO[Long] =
      Files[IO].walk(dir)
        .evalMap(p => Files[IO].getBasicFileAttributes(p))
        .map(_.size)
        .compile.foldMonoid

   def scalaLinesIn(dir: Path): IO[Long] =
      Files[IO].walk(dir)
        .filter(_.extName == ".scala")
        .evalMap(lineCount)
        .compile.foldMonoid


object Fs2WalRe:
   /*§§
   Stream.eval(mache setup), mask für: Exception -> Streamende
   dann flatMap für Zielstream (mit einem ELement)
    */
   def walkSimple[F[_] : Files](start: Path): Stream[F, Path] =
      Stream.eval(Files[F].getBasicFileAttributes(start)).mask.flatMap: attr =>
         val next =
            if attr.isDirectory then
               Files[F].list(start).mask.flatMap(walkSimple)
            else Stream.empty
         Stream.emit(start) ++ next


   /*§§
   cats..Resource aus Resource.fromAutoCloseable( Sync[F].blocking:java.AutoCloseable )
   -> Resource zu fs2.Stream mit Stream.resource( ..)
   fs2.Stream aus java...Stream jstream mit Stream.fromBlockingIterator(jstream.iterator().asScala,..)
    */
   def jwalk[F[_] : Sync](start: Path, chunkSize: Int): Stream[F, Path] =
      import java.nio.file.{Files as JFiles}
      def doWalk = Sync[F].blocking(JFiles.walk(start.toNioPath))

      Stream.resource(Resource.fromAutoCloseable(doWalk))
        .flatMap(jstr => Stream.fromBlockingIterator(jstr.iterator().asScala, chunkSize))
        .map(jpath => Path.fromNioPath(jpath))


   import fs2.Chunk

   /* §§
   imperativ/eager Chunk füllen mit JFiles.walkFileTree
   jedoch interruptible mit Stream.eval(Sync[F].interruptible:imperativ/eager)
   Chunk zu Stream aus einzelnen Elementen mit Stream.chunk
    */
   def walkEager[F[_] : Sync](start: Path): Stream[F, Path] =
      import java.io.IOException
      import java.nio.file.{Files as JFiles, FileVisitor, FileVisitResult, Path as JPath}
      import java.nio.file.attribute.{BasicFileAttributes as JBasicFileAttributes}

      val doWalk = Sync[F].interruptible:
         val bldr = Vector.newBuilder[Path]
         JFiles.walkFileTree(
            start.toNioPath,
            new FileVisitor[JPath]:
               private def enqueue(path: JPath, attrs: JBasicFileAttributes): FileVisitResult =
                  bldr += Path.fromNioPath(path)
                  FileVisitResult.CONTINUE

               override def visitFile(file: JPath, attrs: JBasicFileAttributes): FileVisitResult =
                  if Thread.interrupted() then FileVisitResult.TERMINATE else enqueue(file, attrs)

               override def visitFileFailed(file: JPath, t: IOException): FileVisitResult =
                  FileVisitResult.CONTINUE

               override def preVisitDirectory(dir: JPath, attrs: JBasicFileAttributes): FileVisitResult =
                  if Thread.interrupted() then FileVisitResult.TERMINATE else enqueue(dir, attrs)

               override def postVisitDirectory(dir: JPath, t: IOException): FileVisitResult =
                  if Thread.interrupted() then FileVisitResult.TERMINATE else FileVisitResult.CONTINUE
         )

         Chunk.vector(bldr.result())

      Stream.eval(doWalk).flatMap(Stream.chunk)


   import cats.effect.Async

   /*§§
    Consumer channel.stream - concurrently -
         Producer Stream.eval: Sync[F].interruptible: impure:JFiles.walkFileTree ->
           Visitor:dispatcher.unsafeRunSync(channel.send(... : pure 'channel.send ' zu/in impure per dispatcher.unsafeRunSync
           "sematical blocking channel.send..." per  dispatcher.unsafeRunSync impure und blockiert impure thread JFiles.walk..
           d.h. impure walkFileTree wird kontrolliert durch: pure 'channel.send ' vermöge dispatcher.unsafeRunSync

   -> Koordination/Synchronisation per channel
   main Kontrolle hat Consumer channel.stream
    */
   def walkLazy[F[_] : Async](start: Path, chunkSize: Int): Stream[F, Path] =
      import java.io.IOException
      import java.nio.file.{Files as JFiles, FileVisitor, FileVisitResult, Path as JPath}
      import java.nio.file.attribute.{BasicFileAttributes as JBasicFileAttributes}

      import cats.effect.std.Dispatcher
      import fs2.concurrent.Channel

      def doWalk(dispatcher: Dispatcher[F], channel: Channel[F, Chunk[Path]]) = Sync[F].interruptible:
         val bldr = Vector.newBuilder[Path]
         var size = 0
         JFiles.walkFileTree(
            start.toNioPath,
            new FileVisitor[JPath]:
               private def enqueue(path: JPath, attrs: JBasicFileAttributes): FileVisitResult =
                  bldr += Path.fromNioPath(path)
                  size += 1
                  if size >= chunkSize then
                     val result = dispatcher.unsafeRunSync(channel.send(Chunk.vector(bldr.result())))
                     bldr.clear()
                     size = 0
                     if result.isLeft then FileVisitResult.TERMINATE else FileVisitResult.CONTINUE
                  else FileVisitResult.CONTINUE

               override def visitFile(file: JPath, attrs: JBasicFileAttributes): FileVisitResult =
                  if Thread.interrupted() then FileVisitResult.TERMINATE else enqueue(file, attrs)

               override def visitFileFailed(file: JPath, t: IOException): FileVisitResult =
                  FileVisitResult.CONTINUE

               override def preVisitDirectory(dir: JPath, attrs: JBasicFileAttributes): FileVisitResult =
                  if Thread.interrupted() then FileVisitResult.TERMINATE else enqueue(dir, attrs)

               override def postVisitDirectory(dir: JPath, t: IOException): FileVisitResult =
                  if Thread.interrupted() then FileVisitResult.TERMINATE else FileVisitResult.CONTINUE
         )
         dispatcher.unsafeRunSync(channel.send(Chunk.vector(bldr.result())) >> channel.close)

      Stream.resource(Dispatcher[F]).flatMap:
         dispatcher =>
            Stream.eval(Channel.synchronous[F, Chunk[Path]]).flatMap:
               channel =>
                  channel.stream.flatMap(Stream.chunk).concurrently(Stream.eval(doWalk(dispatcher, channel)))


   /*§§
   impure imperativ "while(acc.size<cS ..)..acc++toWalk.head..toWalk.tail++ descendants aus Using(...list) ..."
   -> Stream(..acc)  ++ Rekursion : per name-Parameter - wird nicht "aufgerufen", nur in Bind/Pull etc. hinterlegt ->
       Fiber/Thread stoppt - Kontrolle hat "wieder" der Aufrufer des Stream
   Zusätzliches Einhausen für interruptible:   Sync[F].interruptible: ... - aushausen: mit  "Stream.eval(.. Sync[F].interruptible: -> imperativ Stream  ).flatten"
    */
   def walkJustInTime[F[_] : Sync](start: Path, chunkSize: Int): Stream[F, Path] =
      import scala.collection.immutable.Queue
      import scala.util.control.NonFatal
      import scala.util.Using
      import java.nio.file.{Files as JFiles, LinkOption}
      import java.nio.file.attribute.{BasicFileAttributes as JBasicFileAttributes}

      case class WalkEntry(
                            path: Path,
                            attr: JBasicFileAttributes
                          )

      def loop(toWalk0: Queue[WalkEntry]): Stream[F, Path] =
         val partialWalk = //Sync[F].interruptible:
            var acc = Vector.empty[Path]
            var toWalk = toWalk0

            while acc.size < chunkSize && toWalk.nonEmpty && !Thread.interrupted() do
               val entry = toWalk.head
               toWalk = toWalk.drop(1)
               acc = acc :+ entry.path
               if entry.attr.isDirectory then
                  Using(JFiles.list(entry.path.toNioPath)):
                     listing =>
                        val descendants = listing.iterator.asScala.flatMap:
                           p =>
                              try
                                 val attr =
                                    JFiles.readAttributes(
                                       p,
                                       classOf[JBasicFileAttributes],
                                       LinkOption.NOFOLLOW_LINKS
                                    )
                                 Some(WalkEntry(Path.fromNioPath(p), attr))
                              catch case NonFatal(_) => None
                        toWalk = Queue.empty ++ descendants ++ toWalk

            Stream.chunk(Chunk.vector(acc)) ++ (if toWalk.isEmpty then Stream.empty else loop(toWalk))

         partialWalk
      //      Stream.eval(partialWalk).flatten

      Stream
        .eval(Sync[F].interruptible:
           WalkEntry(
              start,
              JFiles.readAttributes(start.toNioPath, classOf[JBasicFileAttributes])
           )
        )
        .mask
        .flatMap(w => loop(Queue(w)))


   def totalSize(dir: Path): IO[Long] =
      Files[IO].walk(dir)
        .evalMap(p => Files[IO].getBasicFileAttributes(p))
        .map(_.size)
        .compile.foldMonoid

   //  def totalSizeOptimized(dir: Path): IO[Long] =
   //    Files[IO].walkWithAttributes(dir)
   //      .map(_.attributes.size)
   //      .compile.foldMonoid


   def time[A](f: => A): (FiniteDuration, A) =
      val start = System.currentTimeMillis
      val result = f
      val elapsed = (System.currentTimeMillis - start).millis
      (elapsed, result)

   /*§§
   flatMap: d => großer Block eigenen val und def
    */
   def makeLargeDir: IO[Path] =
      Files[IO].createTempDirectory.flatMap:
         dir =>
            val MaxDepth = 2
            val Names = 'A'.to('E').toList.map(_.toString)

            def loop(cwd: Path, namels: String, depth: Int): IO[Unit] =
               if depth < MaxDepth then
                  Names.traverse_ :
                     name =>
                        val nname = namels + "_" + name
                        val p = cwd / nname
                        Files[IO].createDirectory(p) >>
                          loop(p, nname, depth + 1)
               else if (depth == MaxDepth)
                  Names.traverse_ :
                     name =>
                        Files[IO].createFile(cwd / (namels + "_" + name))
               else IO.unit

            loop(dir, "", 0).as(dir)


   def makeLargeDirStreaming: Stream[IO, Path] =
      Stream.eval(Files[IO].createTempDirectory).flatMap:
         dir =>
            val MaxDepth = 2
            val Names = Stream.emits('A'.to('C').toList.map(_.toString))

            def loop(cwd: Path, namels: String, depth: Int): Stream[IO, Path] =
               Names.flatMap:
                  name =>
                     val nname = namels + "_" + name
                     val p = cwd / nname
                     val nextDir = Stream.eval(Files[IO].createDirectory(p)) >> Stream(p).covary[IO]
                     if depth < MaxDepth then nextDir ++ loop(p, nname, depth + 1)
                     else if (depth == MaxDepth)  nextDir
                     else Stream.empty

            loop(dir, "", 0)


/*§§
Test für/mit Effekt
 */
class Fs2WalReT extends CatsEffectSuite with ScalaCheckEffectSuite:

   import Fs2WalRe.*

   test("walkSimple") {
      val res =
         for
            fi <- makeLargeDir
            v <- walkSimple[IO](fi).evalTap(fi => IO(print(s"\nWalking file: ${fi.absolute.toString}"))).compile.toList
         yield v
      res >>= (li => li.traverse_(fi => IO(print(s"\n Result file: ${fi.toNioPath.toString}"))))
   }

   test("walkJustInTime") {
      val fi = Path("""C:\Users\HANSG_~1\AppData\Local\Temp\2197622863620714710""")
      val res =
         for
            v <- walkJustInTime[IO](fi, 20).evalTap(fi => IO(print(s"\nWalking file: ${fi.absolute.toString}"))).compile.toList
         yield v
      res >>= (li => li.traverse_(fi => IO(print(s"\n Result file: ${fi.toNioPath.toString}"))))
   }

   test("makeLargeDir") {
      val str = makeLargeDirStreaming.evalMap:
         (fi: Path) => IO(print(s"\n Erzeugtes File: ${fi.toNioPath.toString}"))
      str.compile.drain
   }



