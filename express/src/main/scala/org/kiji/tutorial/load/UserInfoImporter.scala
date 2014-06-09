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
import org.kiji.tutorial.avro.Person
import org.kiji.tutorial.avro.Gender
import org.kiji.schema.KijiURI
import org.kiji.tutorial.MovieJob

/**
 * Populates all of our information about various users.
 *
 * Reads in a file with records of the form: `user_id | age | gender | occupation | zipcode`
 *
 * @param args passed in from the command line.
 */
class UserInfoImporter(args: Args) extends MovieJob(args) {
  // Get user ratings
  TextLine(args("user-info"))
      .read
      .mapTo('line -> ('entityId, 'userInfo)) { line: String => {
        val tokens: Array[String] = line.split('|')
        val user = tokens(0).toLong
        val age = tokens(1).toInt
        val gender = if (tokens(2) == "M") Gender.MALE else Gender.FEMALE
        val occupation = tokens(3)
        val zipcode = tokens(4).toString // Some zipcodes are British and have letters in them.

        val person = Person.newBuilder()
            .setAge(age)
            .setGender(gender)
            .setOccupation(occupation)
            .setUserId(user)
            .setZipCode(zipcode)
            .build
        (EntityId(user), person)
      } }

      .write(KijiOutput.builder
          .withTableURI(usersUri)
          .withColumns('userInfo -> "info:info")
          .build)
}
