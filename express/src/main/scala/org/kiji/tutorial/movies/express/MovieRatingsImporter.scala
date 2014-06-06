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
import cascading.pipe.Pipe

import org.kiji.express.flow.ColumnFamilyOutputSpec
import org.kiji.express.flow.EntityId
import org.kiji.express.flow.KijiJob
import org.kiji.express.flow.KijiOutput
import org.kiji.tutorial.movies.avro.MovieRating
import org.kiji.schema.KijiURI

/**
 * Populates a table of movie ratings.
 *
 * Reads in a file with records of the form: `user_id item_id rating timestamp`
 *
 * @param args passed in from the command line.
 */
class MovieRatingsImporter(args: Args) extends MovieJob(args) {
  // Get user ratings
  TextLine(args("ratings"))
      .read
      .project('line)
      .mapTo('line -> ('user, 'movie, 'rating, 'timestamp)) {

        line: String => {
          val contents: Array[String] = line.split("\t")
          // Cast the user and movie IDs into longs, rating into int, time into long.
          (contents(0).toLong, contents(1).toLong, contents(2).toInt, contents(3).toLong)
        }
      }

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
