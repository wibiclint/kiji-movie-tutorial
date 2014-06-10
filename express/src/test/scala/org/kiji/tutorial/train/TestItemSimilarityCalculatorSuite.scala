package org.kiji.tutorial.train

import java.io.{FileWriter, BufferedWriter, File}
import com.twitter.scalding._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.kiji.tutorial.MovieSuite
import org.kiji.tutorial.avro.{MovieRating, SortedSimilarities}
import scala.collection.JavaConverters._
import org.apache.hadoop.hbase.HBaseConfiguration
import com.google.common.io.Files
import org.kiji.schema.util.InstanceBuilder
import org.kiji.tutorial.load.UserInfoImporter
import org.apache.commons.io.FileUtils
import org.kiji.schema._
import com.twitter.scalding.Hdfs
import scala.Some
import com.twitter.scalding.IterableSource


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

  test("Test calculating movie similarities.") {
    // Create a temp Kiji and create the table.
    val usersDdl = io.Source.fromInputStream(getClass.getResourceAsStream("/users.ddl")).mkString

    val DONTCARE_TIMESTAMP = 0L;

    // Function to populate the users table.
    def populateUsers(tableBuilder: InstanceBuilder#TableBuilder): Unit = {
      tableBuilder
          .withRow("0")
          .withFamily("ratings")
          .withQualifier("0").withValue(new MovieRating(0L, 2, DONTCARE_TIMESTAMP))
          .withQualifier("1").withValue(new MovieRating(1L, 3, DONTCARE_TIMESTAMP))
          .withQualifier("2").withValue(new MovieRating(2L, 5, DONTCARE_TIMESTAMP))
          .withQualifier("3").withValue(new MovieRating(3L, 1, DONTCARE_TIMESTAMP))
          .withQualifier("4").withValue(new MovieRating(4L, 1, DONTCARE_TIMESTAMP))
          .withRow("1")
          .withFamily("ratings")
          .withQualifier("0").withValue(new MovieRating(0L, 2, DONTCARE_TIMESTAMP))
          .withQualifier("1").withValue(new MovieRating(1L, 3, DONTCARE_TIMESTAMP))
          .withQualifier("2").withValue(new MovieRating(2L, 5, DONTCARE_TIMESTAMP))
          .withQualifier("3").withValue(new MovieRating(3L, 1, DONTCARE_TIMESTAMP))
          .withQualifier("4").withValue(new MovieRating(4L, 1, DONTCARE_TIMESTAMP))
          .withRow("2")
          .withFamily("ratings")
          .withQualifier("0").withValue(new MovieRating(0L, 2, DONTCARE_TIMESTAMP))
          .withQualifier("1").withValue(new MovieRating(1L, 3, DONTCARE_TIMESTAMP))
          .withQualifier("2").withValue(new MovieRating(2L, 5, DONTCARE_TIMESTAMP))
          .withQualifier("3").withValue(new MovieRating(3L, 1, DONTCARE_TIMESTAMP))
          .withQualifier("4").withValue(new MovieRating(4L, 1, DONTCARE_TIMESTAMP))
          .build()
    }

    val usersUri: String = createTableAndPopulateTableAndReturnUri(usersDdl, "users", populateUsers)
    val kiji: Kiji = Kiji.Factory.get().open(
      KijiURI.newBuilder(usersUri).build()
    )

    // We don't have to seed the movies table with any information.
    val moviesDdl = io.Source.fromInputStream(getClass.getResourceAsStream("/movies.ddl")).mkString
    def nullPopulate(tableBuilder: InstanceBuilder#TableBuilder): Unit = { }
    val moviesUri: String = createTableAndPopulateTableAndReturnUri(moviesDdl, "movies", nullPopulate, Some(kiji))

    val args = new Args(Map(
      "kiji" -> List(usersUri)
    ))
    val argsWithMode = Mode.putMode(Hdfs(strict = false, conf = HBaseConfiguration.create()), args)
    val jobTest = new ItemSimilarityCalculator(argsWithMode)
    assert(jobTest.run)

    // Make sure that we have some similarities now!
    val table: KijiTable = kiji.openTable("movies")
    val reader: KijiTableReader = table.openTableReader
    val rowData: KijiRowData = reader.get(
      table.getEntityId("0"),
      KijiDataRequest.create("most_similar", "most_similar")
    )
    assert(rowData != null)
    val sortedSimilarities: SortedSimilarities =
      rowData.getMostRecentValue("most_similar", "most_similar")
    assert(sortedSimilarities != null)

    reader.close()
    table.release()
    kiji.release()
  }
 }

