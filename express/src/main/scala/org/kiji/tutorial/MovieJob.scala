/**
 * Superclass for all of the DirecTV Scalding jobs.
 * Has repeated command-line args, etc.
 */

package org.kiji.tutorial

import com.twitter.scalding._
import org.kiji.express.flow._
import org.kiji.schema.KijiURI
import cascading.pipe.Pipe
import org.kiji.tutorial.avro.{ItemSimilarityScore, SortedSimilarities, MovieRating}
import scala.collection.JavaConverters._

/**
 * Superclass for all of the KijiExpress jobs for this tutorial.  Contains common command-line
 * options and useful methods.
 *
 * @param args Command-line arguments for the job.
 */
abstract class MovieJob(args: Args) extends KijiJob(args) {
  val kijiUri = args("kiji")
  val moviesUri = KijiURI.newBuilder(kijiUri).withTableName("movies").build
  val usersUri = KijiURI.newBuilder(kijiUri).withTableName("users").build
  val modelSize = 40 // TODO: Make an optional command-line argument.
}