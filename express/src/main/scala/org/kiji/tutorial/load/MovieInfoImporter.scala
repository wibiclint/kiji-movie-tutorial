/**
 * (c) Copyright 2014 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kiji.tutorial.load

import com.twitter.scalding.Args
import com.twitter.scalding.TextLine
import org.kiji.express.flow.EntityId
import org.kiji.express.flow.KijiOutput
import org.kiji.tutorial.avro.MovieInfo
import org.kiji.schema.KijiURI
import java.util.Date
import scala.collection.JavaConversions._
import org.kiji.tutorial.MovieJob
import com.twitter.scalding.typed.TDsl


/**
 * Populates movie metadata.
 *
 * Reads in a file with records of the form:
 * movie id | movie title | release date | video release date | IMDb URL | unknown | Action | Adventure | Animation | Children's | Comedy | Crime | Documentary | Drama | Fantasy | Film-Noir | Horror | Musical | Mystery | Romance | Sci-Fi | Thriller | War | Western |
 *
 * @param args passed in from the command line.
 */
class MovieInfoImporter(args: Args) extends MovieJob(args) {
  // Use typed pipes
  import TDsl._

  // Get the movie metadata.
  TextLine(args("movie-info"))
      .read
      .toTypedPipe[String]('line)

      // Skip blank lines
      .filter(!MovieInfoImporter.lineIsBlank(_))

      .map(MovieInfoImporter.parseLine)
      // Now we have a typed pipe of [Long - movieId, MovieInfo - movieInfo]

      .map{x => (EntityId(x._1.toString), x._2)}
      // Now we have a typed pipe of [EntityId - eid, MovieInfo - movieInfo]

      .toPipe('entityId, 'movieInfo)

      .write(KijiOutput.builder
      .withTableURI(moviesUri)
      .withColumns('movieInfo -> "info:info")
      .build)
}

object MovieInfoImporter {
  def parseLine(line: String): (Long, MovieInfo) = {
    val tokens: Array[String] = line.split('|')
    assert(tokens.length == 5 + genreChoices.length, "Line = " + line)
    val movieId = tokens(0).toLong
    val movieTitle = tokens(1)
    val releaseDate = parseDate(tokens(2))
    val videoReleaseDate = parseDate(tokens(3))
    val imdbUrl = if (tokens(4) == "") None else Some(tokens(4))

    // Get all of the 1s or 0s for whether to choose a given genre.
    val genreSelections = tokens.drop(5)
    val genres: Set[String] = genreSelections
        .zip(genreChoices)
        .filter(_._1 == "1")
        .map(_._2)
        .toSet

    val movieInfo = MovieInfo.newBuilder()
        .setGenres(genres.toList)
        .setImdbUrl(if (imdbUrl == None) null else imdbUrl.get)
        .setMovieId(movieId)
        .setTitle(movieTitle)
        .setTheaterReleaseDate(if (releaseDate == None) null else releaseDate.get)
        .setVideoReleaseDate(if (videoReleaseDate == None) null else videoReleaseDate.get)
        .build()
    (movieId, movieInfo)
  }

  def parseDate(dateAsString: String): Option[Long] = {
    if (dateAsString == "") {
      None
    } else {
      val date: Date = dateFormat.parse(dateAsString)
      Some(date.getTime)
    }
  }

  def lineIsBlank(line: String): Boolean = {
    // Pattern for a blank line
    val p = ("^\\s*$").r
    !p.findFirstIn(line).isEmpty
  }

  val dateFormat = new java.text.SimpleDateFormat("dd-MMM-yyyy")

  val genreChoices = List(
    "unknown",
    "Action",
    "Adventure",
    "Animation",
    "Children's",
    "Comedy",
    "Crime",
    "Documentary",
    "Drama",
    "Fantasy",
    "Film-Noir",
    "Horror",
    "Musical",
    "Mystery",
    "Romance",
    "Sci-Fi",
    "Thriller",
    "War",
    "Western")
}
