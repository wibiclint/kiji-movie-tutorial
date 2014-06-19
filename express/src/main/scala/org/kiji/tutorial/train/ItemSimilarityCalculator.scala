package org.kiji.tutorial.train

import com.twitter.scalding.{FieldConversions, TypedPipe, Args}
import org.kiji.tutorial.MovieJob
import cascading.pipe.Pipe
import org.kiji.express.flow._
import com.twitter.scalding.typed.{Grouped, TDsl}
import org.kiji.tutorial.avro.{SortedSimilarities, ItemSimilarityScore, MovieRating}
import org.kiji.modeling.framework.ModelPipeConversions
import scala.collection.JavaConverters.seqAsJavaListConverter

class ItemSimilarityCalculator(args: Args) extends MovieJob(args) with ModelPipeConversions {
  import TDsl._

  // Read all of the movie ratings data.
  val userRatingsPipe = KijiInput.builder
      .withTableURI(usersUri)
      .withColumnSpecs(
        ColumnFamilyInputSpec
            .builder
            .withFamily("ratings")
            .withSchemaSpec(SchemaSpec.Specific(classOf[MovieRating]))
            .build -> 'ratingInfo)
      .build
      .read
      .debug
      .toTypedPipe[(EntityId, Seq[FlowCell[MovieRating]])]('entityId, 'ratingInfo)

      // Turn the entity Id into a user Id and extract the movie and the movie's rating.
      .flatMap { eidAndCells =>
        val (eid, cells: Seq[FlowCell[MovieRating]]) = eidAndCells
        val userId: Long = eid.components(0).asInstanceOf[String].toLong

        // Go from a list of cells to a list of (userId, movieId, rating)
        cells.map { cell: FlowCell[MovieRating] =>
          val movieId: Long = cell.datum.getMovieId
          val rating: Double = cell.datum.getRating.toDouble
          (userId, movieId, rating)
        }
      }

  val itemItemSimilarityPipe = ItemSimilarityCalculator.calculateItemSimilaritiesFromUserRatings(userRatingsPipe)


  // For each item, sort its similar items by similarity and grab the N most-similar items.
  val similarityRecordsPipe =
    ItemSimilarityCalculator.createSortedSimilarityRecords(itemItemSimilarityPipe, modelSize)

  // Convert the movieId to an entityId and write to the Kiji table.
  similarityRecordsPipe
      .debug
      .map { x: (Long, SortedSimilarities) => (EntityId(x._1.toString), x._2) }
      .toPipe('entityId, 'mostSimilar)
      .write(KijiOutput.builder
          .withTableURI(moviesUri)
          .withColumnSpecs(Map('mostSimilar -> QualifiedColumnOutputSpec.builder
              .withColumn("most_similar", "most_similar")
              .withSchemaSpec(SchemaSpec.Specific(classOf[SortedSimilarities]))
              .build))
      .build)
}

/**
 * Companion object with methods for manipulating similarity pipes.  We put these methods here to
 * make testing them easier.
 */
object ItemSimilarityCalculator extends FieldConversions with ModelPipeConversions {
  import TDsl._

  /**
   * Given a pipe of (userId, movieId, rating), creates a pipe of (movieIdA, movieIdB, similarity).
   *
   * Filters our any movie pairs with negative similarity.
   *
   * @param userRatingsPipe of (userId, movieId, rating)
   * @return pipe of (movieIdA, movieIdB, similarity)
   */
  def calculateItemSimilaritiesFromUserRatings(userRatingsPipe: TypedPipe[(Long, Long, Double)]
      ): TypedPipe[(Long, Long, Double)] = {

    // Calculate the adjusted cosine similarity.
    // (requires converting back to an untyped pipe)
    val itemItemSimilarityPipe = userRatingsPipe
        .toPipe('userId, 'movieId, 'rating)
        .adjustedCosineSimilarity[Long, Long](
          ('userId, 'movieId, 'rating) -> ('movieA, 'movieB, 'similarity))
        .toTypedPipe[(Long, Long, Double)]('movieA, 'movieB, 'similarity)
        // Filter out any similarity pairs with negative similarity.
        .filter(_._3 > 0)
    itemItemSimilarityPipe
  }

  /**
   * Given a pipe of (movieIdA, movieIdB, similarity), create sorted (movieIdB, similarity) pairs
   * for every movieIdA and store those pairs in an `SortedSimilarities` object.
   *
   * @param itemItemSimilarityPipe of the form (movieIdA: Long, movieIdB: Long, similarity: Double)
   * @param modelSize number of most-similar items to save
   * @return a pipe of the form (movieId: Long, sortedSimilarities: SortedSimilarities)
   */
  def createSortedSimilarityRecords(
    itemItemSimilarityPipe: TypedPipe[(Long, Long, Double)],
    modelSize: Int = 50): TypedPipe[(Long, SortedSimilarities)] = {

    // For each item, sort its similar items by similarity and grab the N most-similar items.
    val similarityRecordsPipe = itemItemSimilarityPipe
        .groupBy{ itemItemSimilarity: (Long, Long, Double) => itemItemSimilarity._1 }

        // We have Grouped[Long, (Long, Long, Double] = Grouped[itemA, (itemA, itemB, similarity)]
        // Now we can reverse-sort by similarity and take the N most-similar items.
        .sortWithTake(modelSize) {
          (x: (Long, Long, Double), y: (Long, Long, Double)) => x._3 > y._3
        }
        .mapValues{ x: Seq[(Long, Long, Double)] =>
          val simList = x.map{ simTuple => new ItemSimilarityScore(simTuple._2, simTuple._3) }
          new SortedSimilarities(simList.asJava)
        }
    similarityRecordsPipe
  }

}
