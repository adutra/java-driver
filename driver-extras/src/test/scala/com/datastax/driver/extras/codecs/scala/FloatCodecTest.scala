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

class FloatCodecTest extends AbstractCodecTest[Float, FloatCodec.type] {

  val codec = FloatCodec

  protected override def getTableDefinitions: util.Collection[String] =
    Lists.newArrayList("CREATE TABLE test (pk int PRIMARY KEY, c float)")

  @DataProvider(name = "serde")
  def parametersForFormattingTests: Array[Array[AnyRef]] = {
    Array[Array[AnyRef]](
      Array(0F.asInstanceOf[AnyRef], 0F.asInstanceOf[AnyRef]),
      Array(-12345.6780F.asInstanceOf[AnyRef], -12345.678F.asInstanceOf[AnyRef]),
      Array(Float.MaxValue.asInstanceOf[AnyRef], Float.MaxValue.asInstanceOf[AnyRef]),
      Array(Float.MinValue.asInstanceOf[AnyRef], Float.MinValue.asInstanceOf[AnyRef])
    )
  }

  @DataProvider(name = "format")
  def parametersForFormat: Array[Array[AnyRef]] = {
    Array[Array[AnyRef]](
      Array(0F.asInstanceOf[AnyRef], "0.0"),
      Array(-12345.6780F.asInstanceOf[AnyRef], "-12345.678"),
      Array(Float.MaxValue.asInstanceOf[AnyRef], Float.MaxValue.toString),
      Array(Float.MinValue.asInstanceOf[AnyRef], Float.MinValue.toString)
    )
  }

  @DataProvider(name = "parse")
  def parametersForParse: Array[Array[AnyRef]] = {
    Array[Array[AnyRef]](
      Array("0", 0F.asInstanceOf[AnyRef]),
      Array("1", 1F.asInstanceOf[AnyRef]),
      Array("-12345.6780", -12345.678F.asInstanceOf[AnyRef]),
      Array("", 0F.asInstanceOf[AnyRef]),
      Array("NULL", 0F.asInstanceOf[AnyRef])
    )
  }

}
