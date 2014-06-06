package org.kiji.tutorial.movies.express

import org.scalatest.junit.AssertionsForJUnit
import scala.collection.mutable.ListBuffer
import org.junit.Assert._
import org.junit.Test
import org.junit.Before

/**
 * Tests for MovieInfo companion object's utility functions.
 */
class MovieInfoUtilSuite extends AssertionsForJUnit {
  @Test
  def testParseLine() {
    val line: String = "1|Toy Story (1995)|01-Jan-1995||http://us.imdb.com/M/title-exact?Toy%20Story%20(1995)|0|0|0|1|1|1|0|0|0|0|0|0|0|0|0|0|0|0|0"
    val (
        movieId: Long,
        movieTitle: String,
        releaseDate: Long,
        videoDate: Option[Long],
        imdbUrl: String,
        genres: Set[String]) = MovieInfoImporter.parseLine(line)
    assertEquals(1L, movieId)
    assertEquals("Toy Story (1995)", movieTitle)
    assertEquals(788947200000L, releaseDate)
    assertEquals(None, videoDate)
    assertEquals("http://us.imdb.com/M/title-exact?Toy%20Story%20(1995)", imdbUrl)
    assertEquals(Set("Animation", "Children's", "Comedy"), genres)
  }

}
