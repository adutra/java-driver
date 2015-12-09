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

import java.nio.ByteBuffer

import com.datastax.driver.core.CCMBridge.PerClassSingleNodeCluster.session
import com.datastax.driver.core.ProtocolVersion.V4
import com.datastax.driver.core._
import com.datastax.driver.core.exceptions.InvalidTypeException
import org.assertj.core.api.Assertions._
import org.testng.annotations.Test

abstract class AbstractCodecTest[T, C <: TypeCodec[T]] extends CCMBridge.PerClassSingleNodeCluster {

  val codec: C

  protected override def configure(builder: Cluster.Builder): Cluster.Builder = {
    builder.withCodecRegistry(new CodecRegistry().register(codec))
  }

  @Test(groups = Array("short"), dataProvider = "serde")
  def should_store_and_retrieve_seq(input: T, expected: T) {
    val ps = session.prepare("INSERT INTO test (pk, c) VALUES (?, ?)")
    val bs = ps.bind.setInt(0, 1).set("c", input, codec.getJavaType)
    session.execute(bs)
    val row = session.execute("SELECT c FROM test WHERE pk = 1").one
    val actual = row.get("c", codec.getJavaType)
    assertThat(actual).isEqualTo(expected)
  }

  @Test(groups = Array("unit"), dataProvider = "serde")
  def should_serialize_and_deserialize_seq(input: T, expected: T) {
    assertThat(codec.deserialize(codec.serialize(input, V4), V4)).isEqualTo(expected)
  }

  @Test(groups = Array("unit"), dataProvider = "format")
  def should_format_seq(input: T, expected: String) {
    assertThat(codec.format(input)).isEqualTo(expected)
  }

  @Test(groups = Array("unit"), dataProvider = "parse")
  def should_parse_seq(input: String, expected: T) {
    assertThat(codec.parse(input)).isEqualTo(expected)
  }

  @Test(groups = Array("unit"), expectedExceptions = Array(classOf[InvalidTypeException]))
  def should_not_deserialize_invalid_seq() {
    codec.deserialize(ByteBuffer.allocate(1), V4)
  }

  @Test(groups = Array("unit"), expectedExceptions = Array(classOf[InvalidTypeException]))
  def should_not_parse_invalid_seq() {
    codec.parse("Not a valid input")
  }

}
