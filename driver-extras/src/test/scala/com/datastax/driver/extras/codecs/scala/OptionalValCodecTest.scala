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

import com.google.common.collect.Lists
import org.testng.annotations.DataProvider

class OptionalValCodecTest extends AbstractCodecTest[Option[Int], OptionalValCodec[Int]] {

  val codec = OptionalValCodec[Int](IntCodec)

  protected override def getTableDefinitions: util.Collection[String] =
    Lists.newArrayList("CREATE TABLE test (pk int PRIMARY KEY, c int)")

  @DataProvider(name = "serde")
  def parametersForFormattingTests: Array[Array[AnyRef]] = {
    Array[Array[AnyRef]](
      Array(Some(0), Some(0)),
      Array(None, None),
      Array(Some(Int.MaxValue), Some(Int.MaxValue)),
      Array(Some(Int.MinValue), Some(Int.MinValue))
    )
  }

  @DataProvider(name = "format")
  def parametersForFormat: Array[Array[AnyRef]] = {
    Array[Array[AnyRef]](
      Array(Some(0), "0"),
      Array(None, "NULL"),
      Array(Some(Int.MaxValue), Int.MaxValue.toString),
      Array(Some(Int.MinValue), Int.MinValue.toString)
    )
  }

  @DataProvider(name = "parse")
  def parametersForParse: Array[Array[AnyRef]] = {
    Array[Array[AnyRef]](
      Array("0", Some(0)),
      Array("1", Some(1)),
      Array("-12345", Some(-12345)),
      Array("", None),
      Array("NULL", None)
    )
  }

}
