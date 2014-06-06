package org.kiji.tutorial.movies.express

import com.google.common.io.Files
import java.io.{FileInputStream, FileWriter, BufferedWriter, File}
import com.twitter.scalding.{IterableSource, Hdfs, Mode, Args}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.commons.io.FileUtils
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.kiji.schema.KijiClientTest
import org.kiji.schema.util.InstanceBuilder

@RunWith(classOf[JUnitRunner])
class TestUserInfoImporterSuite extends MovieSuite {
  test("Test importing user information.") {
    // Create the input file.
    val outputDir = Files.createTempDir()
    val csvFile = new File(outputDir.getAbsolutePath + "/test_data.txt")
    val bw = new BufferedWriter(new FileWriter(csvFile))
    bw.write(""" |1|24|M|technician|85711
                 |2|53|F|other|94043
                 |3|23|M|writer|32067
                 |4|24|M|technician|43537
                 |5|33|F|other|15213
                 |6|42|M|executive|98101
                 |7|57|M|administrator|91344
                 |8|36|M|administrator|05201
                 |9|29|M|student|01002""".stripMargin)
    bw.close()

    // Create a temp Kiji and create the table.
    val ddl = io.Source.fromInputStream(getClass.getResourceAsStream("/users.ddl")).mkString
    def nullPopulate(tableBuilder: InstanceBuilder#TableBuilder): Unit = { }

    val tableUri: String = createTableAndPopulateTableAndReturnUri(ddl, "users", nullPopulate)

    val args = new Args(Map(
      "user-info" -> List(csvFile.getAbsolutePath),
      "kiji" -> List(tableUri)
    ))
    val argsWithMode = Mode.putMode(Hdfs(strict = false, conf = HBaseConfiguration.create()), args)
    val jobTest = new UserInfoImporter(argsWithMode)
    jobTest.run
    FileUtils.deleteDirectory(outputDir)
  }
}
