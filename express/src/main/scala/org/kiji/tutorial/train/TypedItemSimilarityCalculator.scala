package org.kiji.tutorial.train

import com.twitter.scalding.{TypedPipe, Args}
import org.kiji.tutorial.MovieJob
import cascading.pipe.Pipe
import org.kiji.express.flow._
import com.twitter.scalding.typed.{Grouped, TDsl}
import org.kiji.tutorial.avro.{SortedSimilarities, ItemSimilarityScore, MovieRating}
import org.kiji.modeling.framework.ModelPipeConversions
import scala.collection.JavaConverters.seqAsJavaListConverter

class TypedItemSimilarityCalculator(args: Args) extends MovieJob(args) with ModelPipeConversions {
  import TDsl._

  // Read all of the movie ratings data.
  val userRatingsPipe = KijiInput.builder
      .withTableURI(usersUri)
      .withColumns("ratings:ratings" -> 'ratingInfo)
      .build
      .read
      .toTypedPipe[(EntityId, Seq[FlowCell[MovieRating]])]('entityId, 'ratingInfo)

      // Turn the entity Id into a user Id and extract the movie and the movie's rating.
      .map { eidAndCells =>
        val (eid, cells: Seq[FlowCell[MovieRating]]) = eidAndCells
        val userId: Long = eid.components(0).asInstanceOf[Long]
        assert(cells.length == 1, "Should have only one review per user per movie")

        val cell: FlowCell[MovieRating] = cells(0)
        val movieId: Long = cell.datum.getMovieId
        val rating: Double = cell.datum.getRating.toDouble
        (userId, movieId, rating)
      }

  // Calculate the adjusted cosine similarity.
  // (requires converting back to an untyped pipe)
  val itemItemSimilarityPipe = userRatingsPipe
      .toPipe('userId, 'movieId, 'rating)
      .adjustedCosineSimilarity[Long, Long](
        ('userId, 'movieId, 'rating) -> ('movieA, 'movieB, 'similarity))
      .toTypedPipe[(Long, Long, Double)]('movieA, 'movieB, 'similarity)
      // Filter out any similarity pairs with negative similarity.
      .filter(_._3 > 0)

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

  // Convert the movieId to an entityId and write to the Kiji table.
  similarityRecordsPipe
      .map { x: (Long, SortedSimilarities) => (EntityId(x._1), x._2) }
      .toPipe('entityId, 'mostSimilar)
      .write(KijiOutput.builder
          .withTableURI(moviesUri)
          .withColumnSpecs(Map('mostSimilar -> QualifiedColumnOutputSpec.builder
              .withColumn("most_similar", "most_similar")
              .withSchemaSpec(SchemaSpec.Specific(classOf[SortedSimilarities]))
              .build))
      .build)
}
