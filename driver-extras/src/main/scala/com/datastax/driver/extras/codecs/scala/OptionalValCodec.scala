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

import com.datastax.driver.core.{DataType, ProtocolVersion, TypeCodec}
import com.google.common.reflect.TypeToken

class OptionalValCodec[T <: AnyVal](innerCodec: TypeCodec[T], cqlType: DataType, javaType: TypeToken[Option[T]]) extends TypeCodec[Option[T]](cqlType, javaType) {

  override def serialize(value: Option[T], protocolVersion: ProtocolVersion): ByteBuffer =
    if (value.isEmpty) null else innerCodec.serialize(value.get, protocolVersion)

  override def deserialize(bytes: ByteBuffer, protocolVersion: ProtocolVersion): Option[T] =
    if (bytes == null || bytes.remaining() == 0) None else Option(innerCodec.deserialize(bytes, protocolVersion))

  override def format(value: Option[T]): String =
    if (value.isEmpty) "NULL" else innerCodec.format(value.get)

  override def parse(value: String): Option[T] =
    if (value == null || value.isEmpty || value.equalsIgnoreCase("NULL")) None else Option(innerCodec.parse(value))

}

object OptionalValCodec {

  def apply[T <: AnyVal](innerCodec: TypeCodec[T]) =
    new OptionalValCodec[T](innerCodec, innerCodec.getCqlType, TypeTokens.optionOf(innerCodec.getJavaType))

}
