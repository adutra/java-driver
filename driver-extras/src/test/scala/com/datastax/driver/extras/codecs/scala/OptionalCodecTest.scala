/*
 *      Copyright (C) 2012-2015 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.extras.codecs.scala

import java.util

import com.datastax.driver.core.TypeCodec.varchar
import com.google.common.collect.Lists
import org.testng.annotations.{DataProvider, Test}

class OptionalCodecTest extends AbstractCodecTest[Option[String], OptionalCodec[String]] {

  val codec = OptionalCodec[String](varchar())

  protected override def getTableDefinitions: util.Collection[String] =
    Lists.newArrayList("CREATE TABLE test (pk int PRIMARY KEY, c text)")

  @DataProvider(name = "serde")
  def parametersForFormattingTests: Array[Array[AnyRef]] = {
    Array[Array[AnyRef]](
      Array(Some("foo"), Some("foo")),
      Array(None, None),
      Array(Some(""), Some(""))
    )
  }

  @DataProvider(name = "format")
  def parametersForFormat: Array[Array[AnyRef]] = {
    Array[Array[AnyRef]](
      Array(Some("foo"), "'foo'"),
      Array(None, "NULL"),
      Array(Some(""), "''")
    )
  }

  @DataProvider(name = "parse")
  def parametersForParse: Array[Array[AnyRef]] = {
    Array[Array[AnyRef]](
      Array("'foo'", Some("foo")),
      Array("''", Some("")),
      Array("NULL", None)
    )
  }

  @Test(groups = Array("unit"))
  override def should_not_deserialize_invalid_seq() {
    // no invalid sequence
  }

}
