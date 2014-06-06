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

package org.kiji.tutorial.movies.express

import com.twitter.scalding.Args
import com.twitter.scalding.TextLine
import org.kiji.express.flow.EntityId
import org.kiji.express.flow.KijiOutput
import org.kiji.tutorial.movies.avro.MovieRating
import org.kiji.schema.KijiURI
import java.util.Date

/**
 * Populates movie metadata.
 *
 * Reads in a file with records of the form:
 * movie id | movie title | release date | video release date | IMDb URL | unknown | Action | Adventure | Animation | Children's | Comedy | Crime | Documentary | Drama | Fantasy | Film-Noir | Horror | Musical | Mystery | Romance | Sci-Fi | Thriller | War | Western |
 *
 * @param args passed in from the command line.
 */
class MovieInfoImporter(args: Args) extends MovieJob(args) {
  // Get user ratings
  TextLine(args("movie-info"))
      .read
      .project('line)
      .mapTo('line -> ('movieId, 'movieTitle, 'releaseDate, 'videoReleaseDate, 'imdbUrl, 'genres))( MovieInfoImporter.parseLine)

      // Mark the user as the entityId
      .map('user -> 'entityId) { user: Long => EntityId(user) }

      // Create a MovieRating Avro item
      .map(('movie, 'rating, 'timestamp) -> 'movieRating) {
        x: (Long, Int, Long) =>
            val (movie, rating, timestamp) = x
            MovieRating
            .newBuilder()
            .setMovieId(movie)
            .setRating(rating)
            .setTimestamp(timestamp).build
      }

      .write(KijiOutput.builder
          .withTableURI(
            KijiURI.newBuilder(kijiUri).withTableName("users").build()
          )
          .withColumns('movieRating -> "ratings:ratings")
          .withTimestampField('timestamp)
          .build)
}

object MovieInfoImporter {
  def parseLine(line: String): (Long, String, Long, Option[Long], String, Set[String]) = {
    val tokens: Array[String] = line.split('|')
    val movieId = tokens(0).toLong
    val movieTitle = tokens(1)
    val releaseDate = dateStringToLong(tokens(2))
    val videoReleaseDate = if (tokens(3) == "") None else Some(dateStringToLong(tokens(3)))
    val imdbUrl = tokens(4)
    assert(imdbUrl.startsWith("http://"), "Weird URL! " + imdbUrl)

    // Get all of the 1s or 0s for whether to choose a given genre.
    val genreSelections = tokens.drop(5)
    val genres: Set[String] = genreSelections
        .zip(genreChoices)
        .filter(_._1 == "1")
        .map(_._2)
        .toSet
    (movieId, movieTitle, releaseDate, videoReleaseDate, imdbUrl, genres)
  }

  def dateStringToLong(dateAsString: String): Long = {
    val date: Date = dateFormat.parse(dateAsString)
    date.getTime
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
