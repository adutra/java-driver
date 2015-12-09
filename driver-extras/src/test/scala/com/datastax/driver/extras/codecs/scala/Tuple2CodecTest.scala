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

import com.datastax.driver.core.CodecRegistry.DEFAULT_INSTANCE
import com.datastax.driver.core.DataType._
import com.datastax.driver.core.ProtocolVersion.V4
import com.datastax.driver.core.{TupleType, TypeCodec}
import com.google.common.collect.Lists
import org.testng.annotations.DataProvider


class Tuple2CodecTest extends AbstractCodecTest[(BigInt, String), Tuple2Codec[BigInt, String]] {

  val codec = Tuple2Codec(TupleType.of(V4, DEFAULT_INSTANCE, varint(), varchar()), BigIntCodec, TypeCodec.varchar)

  protected override def getTableDefinitions: util.Collection[String] =
    Lists.newArrayList("CREATE TABLE test (pk int PRIMARY KEY, c tuple<varint,text>)")

  @DataProvider(name = "serde")
  def parametersForFormattingTests: Array[Array[AnyRef]] = {
    Array[Array[AnyRef]](
      Array(Tuple2(null, null), Tuple2(null, null)),
      Array(null, null),
      Array(Tuple2(BigInt(0), "foo"), Tuple2(0, "foo"))
    )
  }

  @DataProvider(name = "format")
  def parametersForFormat: Array[Array[AnyRef]] = {
    Array[Array[AnyRef]](
      Array(Tuple2(null, null), "(NULL,NULL)"),
      Array(Tuple2(BigInt(0), "foo"), "(0,'foo')"),
      Array(null, "NULL")
    )
  }

  @DataProvider(name = "parse")
  def parametersForParse: Array[Array[AnyRef]] = {
    Array[Array[AnyRef]](
      Array("( null , null )", Tuple2(null, null)),
      Array("(0,'foo')", Tuple2(BigInt(0), "foo")),
      Array("", null),
      Array("NULL", null)
    )
  }

}
