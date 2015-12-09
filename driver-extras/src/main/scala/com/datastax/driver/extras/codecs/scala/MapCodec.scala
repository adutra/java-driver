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

import java.nio.{BufferUnderflowException, ByteBuffer}

import com.datastax.driver.core.DataType.CollectionType
import com.datastax.driver.core._
import com.datastax.driver.core.exceptions.InvalidTypeException
import com.google.common.reflect.TypeToken

import scala.collection.mutable.ArrayBuffer

class MapCodec[K, V](cqlType: CollectionType, javaType: TypeToken[Map[K, V]], keyCodec: TypeCodec[K], valueCodec: TypeCodec[V]) extends TypeCodec[Map[K, V]](cqlType, javaType) {

  def serialize(value: Map[K, V], protocolVersion: ProtocolVersion): ByteBuffer = {
    if (value == null) return null
    val bbs = new ArrayBuffer[ByteBuffer](2 * value.size)
    for ((k, v) <- value) {
      if (k == null) {
        throw new NullPointerException("Map keys cannot be null")
      }
      if (v == null) {
        throw new NullPointerException("Map values cannot be null")
      }
      try {
        bbs += keyCodec.serialize(k, protocolVersion)
      }
      catch {
        case e: ClassCastException =>
          throw new InvalidTypeException( s"""Invalid type for map key, expecting ${keyCodec.getJavaType} but got ${k.getClass}""", e)
      }
      try {
        bbs += valueCodec.serialize(v, protocolVersion)
      }
      catch {
        case e: ClassCastException =>
          throw new InvalidTypeException( s"""Invalid type for map value, expecting ${valueCodec.getJavaType} but got ${v.getClass}""", e)
      }
    }
    CodecUtils.pack(bbs.toArray, value.size, protocolVersion)
  }

  def deserialize(bytes: ByteBuffer, protocolVersion: ProtocolVersion): Map[K, V] = {
    if (bytes == null || bytes.remaining == 0) return Map.empty[K, V]
    try {
      val input = bytes.duplicate
      val n = CodecUtils.readSize(input, protocolVersion)
      val m = Map.newBuilder[K, V]
      for (_ <- 1 to n) {
        val k = keyCodec.deserialize(CodecUtils.readValue(input, protocolVersion), protocolVersion)
        val v = valueCodec.deserialize(CodecUtils.readValue(input, protocolVersion), protocolVersion)
        m += ((k, v))
      }
      m.result()
    }
    catch {
      case e: BufferUnderflowException =>
        throw new InvalidTypeException("Not enough bytes to deserialize a map", e)
    }
  }

  def format(value: Map[K, V]): String = {
    if (value == null) "NULL" else '{' + value.map { case (k, v) => s"${keyCodec.format(k)}:${valueCodec.format(v)}" }.mkString(",") + '}'
  }

  def parse(value: String): Map[K, V] = {
    if (value == null || value.isEmpty || value.equalsIgnoreCase("NULL")) return Map.empty[K, V]
    var idx = ParseUtils.skipSpaces(value, 0)
    if (value.charAt(idx) != '{') throw new InvalidTypeException( s"""Cannot parse map value from "$value", at character $idx expecting '{' but got '${value.charAt(idx)}'""")
    idx = ParseUtils.skipSpaces(value, idx + 1)
    val m = Map.newBuilder[K, V]
    if (value.charAt(idx) == '}') return m.result()
    while (idx < value.length) {
      var n: Int = 0
      try {
        n = ParseUtils.skipCQLValue(value, idx)
      }
      catch {
        case e: IllegalArgumentException =>
          throw new InvalidTypeException( s"""Cannot parse map value from "$value", invalid CQL value at character $idx""", e)
      }
      val k = keyCodec.parse(value.substring(idx, n))
      idx = n
      idx = ParseUtils.skipSpaces(value, idx)
      if (value.charAt(idx) != ':') throw new InvalidTypeException( s"""Cannot parse map value from "$value", at character $idx expecting ':' but got '${value.charAt(idx)}'""")
      idx = ParseUtils.skipSpaces(value, idx + 1)
      try {
        n = ParseUtils.skipCQLValue(value, idx)
      }
      catch {
        case e: IllegalArgumentException =>
          throw new InvalidTypeException( s"""Cannot parse map value from "$value", invalid CQL value at character $idx""", e)
      }
      val v = valueCodec.parse(value.substring(idx, n))
      m += ((k, v))
      idx = n
      idx = ParseUtils.skipSpaces(value, idx)
      if (value.charAt(idx) == '}') return m.result()
      if (value.charAt(idx) != ',') throw new InvalidTypeException( s"""Cannot parse map value from "$value", at character $idx expecting ',' but got '${value.charAt(idx)}'""")
      idx = ParseUtils.skipSpaces(value, idx + 1)
    }
    throw new InvalidTypeException( s"""Malformed map value "$value", missing closing '}'""")
  }

  override def accepts(value: AnyRef): Boolean = {
    value match {
      case map: Map[_, _] => if (map.isEmpty) true else keyCodec.accepts(map.head._1) && valueCodec.accepts(map.head._2)
      case _ => false
    }
  }

}

object MapCodec {

  def apply[K, V](keyCodec: TypeCodec[K], valueCodec: TypeCodec[V]) = {
    new MapCodec[K, V](DataType.map(keyCodec.getCqlType, valueCodec.getCqlType), TypeTokens.mapOf(keyCodec.getJavaType, valueCodec.getJavaType), keyCodec, valueCodec)
  }

}
