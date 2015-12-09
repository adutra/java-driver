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

import scala.collection.SortedSet

class SetCodecTest extends AbstractCodecTest[Set[Int], SetCodec[Int]] {

  val codec = SetCodec(IntCodec)

  protected override def getTableDefinitions: util.Collection[String] =
    Lists.newArrayList("CREATE TABLE test (pk int PRIMARY KEY, c set<int>)")

  @DataProvider(name = "serde")
  def parametersForFormattingTests: Array[Array[AnyRef]] = {
    Array[Array[AnyRef]](
      Array(Set(), Set()),
      Array(null, Set()),
      Array(Set(-1, 0, 1), Set(-1, 0, 1))
    )
  }

  @DataProvider(name = "format")
  def parametersForFormat: Array[Array[AnyRef]] = {
    Array[Array[AnyRef]](
      Array(Set(), "{}"),
      Array(SortedSet(-1, 0, 1), "{-1,0,1}"),
      Array(null, "NULL")
    )
  }

  @DataProvider(name = "parse")
  def parametersForParse: Array[Array[AnyRef]] = {
    Array[Array[AnyRef]](
      Array("{}", Set()),
      Array("{1,0,-1}", Set(-1, 0, 1)),
      Array("", Set()),
      Array("NULL", Set())
    )
  }

}
