/**
 * Superclass for all of the DirecTV Scalding jobs.
 * Has repeated command-line args, etc.
 */

package org.kiji.tutorial.movies.express

import com.twitter.scalding._
import org.kiji.express.flow._

class MovieJob(args: Args) extends KijiJob(args) {
  val kijiUri = args("kiji")
}