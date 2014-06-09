package org.kiji.tutorial.train

import java.io.File
import com.twitter.scalding._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.kiji.tutorial.MovieSuite
import org.kiji.tutorial.avro.SortedSimilarities
import scala.collection.JavaConverters._
import org.apache.hadoop.hbase.HBaseConfiguration


@RunWith(classOf[JUnitRunner])
class TestItemSimilarityCalculatorSuite extends MovieSuite {

  import TDsl._

  test("Test sorting similarities and making records.") {
    class SortedSimilarityRecordsJob(args: Args) extends Job(args) {
      val inputPipe = IterableSource(List(
        (0L, 1L, 0.9),
        (0L, 2L, 0.8),
        (0L, 3L, 0.7),
        (0L, 4L, 0.6),
        (0L, 5L, 0.5)
      ), ('movieA, 'movieB, 'similarity))
          .read
          .toTypedPipe[(Long, Long, Double)]('movieA, 'movieB, 'similarity)

      val outputPipe = ItemSimilarityCalculator.createSortedSimilarityRecords(inputPipe, 4)
      outputPipe
          .map { x: (Long, SortedSimilarities) =>
            val mostSim: List[Long] = x._2.getSimilarities.asScala.toList.map { _.getItem.longValue }
            (x._1, mostSim)
          }
          .toPipe('movieId, 'listOfMostSimilarMovies)
          .assertOutputValues(('movieId, 'listOfMostSimilarMovies), Set(
            (0L, List(1L, 2L, 3L, 4L))
          ))
      }
      val argsWithMode = Mode.putMode(Hdfs(strict = false, conf = HBaseConfiguration.create()), new Args(Map()))
      val jobTest = new SortedSimilarityRecordsJob(argsWithMode)
      assert(jobTest.run)
  }
 }

