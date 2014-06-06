package org.kiji.tutorial.movies.express

import com.google.common.io.Files
import java.io.{FileWriter, BufferedWriter, File}
import com.twitter.scalding.{Hdfs, Mode, Args}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.commons.io.FileUtils
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.kiji.schema.util.InstanceBuilder

@RunWith(classOf[JUnitRunner])
class TestMovieInfoImporterSuite extends MovieSuite {
  test("Test importing movie information.") {
    // Create the input file.
    val outputDir = Files.createTempDir()
    val csvFile = new File(outputDir.getAbsolutePath + "/test_data.txt")
    val bw = new BufferedWriter(new FileWriter(csvFile))
    bw.write(
      """
        |1|Toy Story (1995)|01-Jan-1995||http://us.imdb.com/M/title-exact?Toy%20Story%20(1995)|0|0|0|1|1|1|0|0|0|0|0|0|0|0|0|0|0|0|0
        |2|GoldenEye (1995)|01-Jan-1995||http://us.imdb.com/M/title-exact?GoldenEye%20(1995)|0|1|1|0|0|0|0|0|0|0|0|0|0|0|0|0|1|0|0
        |3|Four Rooms (1995)|01-Jan-1995||http://us.imdb.com/M/title-exact?Four%20Rooms%20(1995)|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|1|0|0
        |4|Get Shorty (1995)|01-Jan-1995||http://us.imdb.com/M/title-exact?Get%20Shorty%20(1995)|0|1|0|0|0|1|0|0|1|0|0|0|0|0|0|0|0|0|0
        |5|Copycat (1995)|01-Jan-1995||http://us.imdb.com/M/title-exact?Copycat%20(1995)|0|0|0|0|0|0|1|0|1|0|0|0|0|0|0|0|1|0|0
        |6|Shanghai Triad (Yao a yao yao dao waipo qiao) (1995)|01-Jan-1995||http://us.imdb.com/Title?Yao+a+yao+yao+dao+waipo+qiao+(1995)|0|0|0|0|0|0|0|0|1|0|0|0|0|0|0|0|0|0|0
        |7|Twelve Monkeys (1995)|01-Jan-1995||http://us.imdb.com/M/title-exact?Twelve%20Monkeys%20(1995)|0|0|0|0|0|0|0|0|1|0|0|0|0|0|0|1|0|0|0
        |8|Babe (1995)|01-Jan-1995||http://us.imdb.com/M/title-exact?Babe%20(1995)|0|0|0|0|1|1|0|0|1|0|0|0|0|0|0|0|0|0|0
        |9|Dead Man Walking (1995)|01-Jan-1995||http://us.imdb.com/M/title-exact?Dead%20Man%20Walking%20(1995)|0|0|0|0|0|0|0|0|1|0|0|0|0|0|0|0|0|0|0
        |10|Richard III (1995)|22-Jan-1996||http://us.imdb.com/M/title-exact?Richard%20III%20(1995)|0|0|0|0|0|0|0|0|1|0|0|0|0|0|0|0|0|1|0
        |11|Seven (Se7en) (1995)|01-Jan-1995||http://us.imdb.com/M/title-exact?Se7en%20(1995)|0|0|0|0|0|0|1|0|0|0|0|0|0|0|0|0|1|0|0
        |267|unknown||||1|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0
      """.stripMargin)
    bw.close()

    // Create a temp Kiji and create the table.
    val ddl = io.Source.fromInputStream(getClass.getResourceAsStream("/content.ddl")).mkString
    def nullPopulate(tableBuilder: InstanceBuilder#TableBuilder): Unit = { }

    val tableUri: String = createTableAndPopulateTableAndReturnUri(ddl, "movies", nullPopulate)

    val args = new Args(Map(
      "movie-info" -> List(csvFile.getAbsolutePath),
      "kiji" -> List(tableUri)
    ))
    val argsWithMode = Mode.putMode(Hdfs(strict = false, conf = HBaseConfiguration.create()), args)
    val jobTest = new MovieInfoImporter(argsWithMode)
    jobTest.run
    FileUtils.deleteDirectory(outputDir)
  }
}
