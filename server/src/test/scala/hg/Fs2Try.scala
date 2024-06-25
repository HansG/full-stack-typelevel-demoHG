package hg

import cats.effect.{IO, IOApp}
import fs2.{Chunk, Stream}
import munit.{CatsEffectSuite, ScalaCheckEffectSuite}

import scala.concurrent.ExecutionContext

class Fs2Try extends CatsEffectSuite with ScalaCheckEffectSuite:

   test("chunkedStream mit split") {
      val sentences: List[String] = List(
         "This is a simple sentence.",
         "Here we start a new chunk.",
         "This sentence follows the previous one.",
         "Another sentence before we again.",
         "start and we go with another chunk.",
         "And finally a last sentence."
      )

      // Define the stream of sentences
      val streamOfSentences: Stream[IO, String] = Stream.emits(sentences).covary[IO]

      // Define the chunking criterion: start new chunk when sentence contains the word "start"
      val chunkedStream: Stream[IO, Chunk[String]] =
         streamOfSentences.split(_.contains("start"))

      // Process and print each chunk
      chunkedStream.evalMap { chunk =>
         IO {
            println(s"Chunk: ${chunk.toArray.mkString(" | ")}")
         }
      }.compile.drain
   }


   import cats.effect.{IO, IOApp, Resource}
   import fs2.{Stream, io, text}
   import java.nio.file.Paths

   test("Write in files") {
      val numbers: List[Int] = List.range(1, 21) // Generating a list of numbers from 1 to 20
      val streamOfNumbers: Stream[IO, Int] = Stream.emits(numbers).covary[IO]

      // Partition the stream into evens and odds
      val (evens, odds) = (streamOfNumbers.filter(n => n % 2 == 0 ), streamOfNumbers.filter(n => n % 2 != 0 ))


   // Sink to write to a file, taking path and stream of Strings
      def writeFile(path: String, stream: Stream[IO, String]): Stream[IO, Unit] = {
         Stream.resource(Resource.fromAutoCloseable(IO(scala.io.Source.fromFile(path))))
            .flatMap { source =>
               stream.
                  .intersperse("\n") // Füge Zeilenumbrüche zwischen den Elementen ein
                  .through(text.utf8.encode) // Kodiere die Strings in UTF-8
                  .through(Files[IO].writeAll(Paths.get(path))) // Schreibe den Stream in die Datei
            }
      }

      def writeToFile(filePath: String, data: Stream[IO, String]): Stream[IO, Unit] = {
         Stream.resource(io.file.Files[IO].createDirectories(Paths.get(filePath).getParent)) >>
            data
               .intersperse("\n")
               .through(text.utf8Encode)
               .through(io.file.Files[IO].writeAll(Paths.get(filePath)))
      }

      // Convert numbers to Strings and write to different files
      val writeEvens = writeFile("./evens.txt", evens.map(_.toString))
      val writeOdds = writeFile("./odds.txt", odds.map(_.toString))

      // Combine the two write operations into one stream and compile to effect, which will execute it
      (writeEvens ++ writeOdds).compile.drain
      // Run both write operations concurrently and wait for both to finish
      writeOdds.merge(writeEvens).compile.drain
   }

