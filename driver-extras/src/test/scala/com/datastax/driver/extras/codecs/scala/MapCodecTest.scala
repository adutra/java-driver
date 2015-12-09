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

import com.datastax.driver.core.TypeCodec
import com.google.common.collect.Lists
import org.testng.annotations.DataProvider

import scala.collection.SortedMap

class MapCodecTest extends AbstractCodecTest[Map[Int, String], MapCodec[Int, String]] {

  val codec = MapCodec(IntCodec, TypeCodec.varchar)

  protected override def getTableDefinitions: util.Collection[String] =
    Lists.newArrayList("CREATE TABLE test (pk int PRIMARY KEY, c map<int,text>)")

  @DataProvider(name = "serde")
  def parametersForFormattingTests: Array[Array[AnyRef]] = {
    Array[Array[AnyRef]](
      Array(Map(), Map()),
      Array(null, Map()),
      Array(Map(0 -> "foo", 1 -> "bar"), Map(0 -> "foo", 1 -> "bar"))
    )
  }

  @DataProvider(name = "format")
  def parametersForFormat: Array[Array[AnyRef]] = {
    Array[Array[AnyRef]](
      Array(Map(), "{}"),
      Array(SortedMap(0 -> "foo", 1 -> "bar"), "{0:'foo',1:'bar'}"),
      Array(null, "NULL")
    )
  }

  @DataProvider(name = "parse")
  def parametersForParse: Array[Array[AnyRef]] = {
    Array[Array[AnyRef]](
      Array("{}", Map()),
      Array("{1:'bar',0:'foo'}", Map(0 -> "foo", 1 -> "bar")),
      Array("", Map()),
      Array("NULL", Map())
    )
  }

}
