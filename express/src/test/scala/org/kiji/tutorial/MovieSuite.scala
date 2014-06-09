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
package org.kiji.tutorial

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.kiji.express.KijiSuite
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.kiji.schema.util.InstanceBuilder
import org.kiji.schema.Kiji
import org.kiji.schema.KijiURI
import org.kiji.schema.shell.api.Client
import org.kiji.schema.KijiTable

@RunWith(classOf[JUnitRunner])
trait MovieSuite extends KijiSuite with TestPipeConversions {
  val logger: Logger = LoggerFactory.getLogger(classOf[MovieSuite])

  /**
   * Utility method to build a Kiji table for testing.  Should eventually go into a KijiExpress
   * testing library.
   */
  def createTableAndPopulateTableAndReturnUri(
     ddl: String,
     tableName: String,
     functionToPopulateTable: InstanceBuilder#TableBuilder => Unit,
     instanceName: String = "default_%s".format(counter.incrementAndGet())
   ): String = {

    val kiji: Kiji = new InstanceBuilder(instanceName).build()
    try {
      // Create the instance
      val kijiUri: KijiURI = kiji.getURI

      val client: Client = Client.newInstance(kijiUri)
      client.executeUpdate(ddl)
      client.close()

      val table: KijiTable = kiji.openTable(tableName)
      try {
        // Populate the table!!!!
        functionToPopulateTable(new InstanceBuilder(kiji).withTable(table))

        table.getURI.toString
      } finally {
        table.release()
      }
    } finally {
      kiji.release()
    }
  }
}

