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

class LongCodecTest extends AbstractCodecTest[Long, LongCodec.type] {

  val codec = LongCodec

  protected override def getTableDefinitions: util.Collection[String] =
    Lists.newArrayList("CREATE TABLE test (pk int PRIMARY KEY, c bigint)")

  @DataProvider(name = "serde")
  def parametersForFormattingTests: Array[Array[AnyRef]] = {
    Array[Array[AnyRef]](
      Array(0L.asInstanceOf[AnyRef], 0L.asInstanceOf[AnyRef]),
      Array(Long.MaxValue.asInstanceOf[AnyRef], Long.MaxValue.asInstanceOf[AnyRef]),
      Array(Long.MinValue.asInstanceOf[AnyRef], Long.MinValue.asInstanceOf[AnyRef])
    )
  }

  @DataProvider(name = "format")
  def parametersForFormat: Array[Array[AnyRef]] = {
    Array[Array[AnyRef]](
      Array(0L.asInstanceOf[AnyRef], "0"),
      Array(Long.MaxValue.asInstanceOf[AnyRef], Long.MaxValue.toString),
      Array(Long.MinValue.asInstanceOf[AnyRef], Long.MinValue.toString)
    )
  }

  @DataProvider(name = "parse")
  def parametersForParse: Array[Array[AnyRef]] = {
    Array[Array[AnyRef]](
      Array("0", 0L.asInstanceOf[AnyRef]),
      Array("1", 1L.asInstanceOf[AnyRef]),
      Array("-12345", -12345L.asInstanceOf[AnyRef]),
      Array("", 0L.asInstanceOf[AnyRef]),
      Array("NULL", 0L.asInstanceOf[AnyRef])
    )
  }

}
