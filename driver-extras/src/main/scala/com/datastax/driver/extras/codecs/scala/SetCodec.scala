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

import com.datastax.driver.core.CodecUtils.readSize
import com.datastax.driver.core.DataType.CollectionType
import com.datastax.driver.core._
import com.datastax.driver.core.exceptions.InvalidTypeException
import com.google.common.reflect.TypeToken

import scala.collection.mutable.ArrayBuffer

class SetCodec[E](cqlType: CollectionType, javaType: TypeToken[Set[E]], eltCodec: TypeCodec[E]) extends TypeCodec[Set[E]](cqlType, javaType) {

  override def serialize(value: Set[E], protocolVersion: ProtocolVersion): ByteBuffer = {
    if (value == null) return null
    val bbs = new ArrayBuffer[ByteBuffer](value.size)
    for (elt <- value) {
      if (elt == null)
        throw new NullPointerException("Set elements cannot be null")
      try {
        bbs += eltCodec.serialize(elt, protocolVersion)
      }
      catch {
        case e: ClassCastException =>
          throw new InvalidTypeException(s"Invalid type for set element, expecting ${eltCodec.getJavaType} but got ${elt.getClass}", e)
      }
    }
    CodecUtils.pack(bbs.toArray, value.size, protocolVersion)
  }

  override def deserialize(bytes: ByteBuffer, protocolVersion: ProtocolVersion): Set[E] = {
    if (bytes == null || bytes.remaining == 0) return Set.empty[E]
    try {
      val input: ByteBuffer = bytes.duplicate
      val n: Int = readSize(input, protocolVersion)
      val s = Set.newBuilder[E]
      for (_ <- 1 to n) {
        val elt = eltCodec.deserialize(CodecUtils.readValue(input, protocolVersion), protocolVersion)
        s += elt
      }
      s.result()
    }
    catch {
      case e: BufferUnderflowException =>
        throw new InvalidTypeException("Not enough bytes to deserialize set", e)
    }
  }

  override def format(value: Set[E]): String = {
    if (value == null) "NULL" else '{' + value.map(e => eltCodec.format(e)).mkString(",") + '}'
  }

  override def parse(value: String): Set[E] = {
    if (value == null || value.isEmpty || value.equalsIgnoreCase("NULL")) return Set.empty[E]
    var idx: Int = ParseUtils.skipSpaces(value, 0)
    if (value.charAt(idx) != '{') throw new InvalidTypeException( s"""Cannot parse set value from "$value", at character $idx expecting '{' but got '${value.charAt(idx)}'""")
    idx = ParseUtils.skipSpaces(value, idx + 1)
    val set: ArrayBuffer[E] = ArrayBuffer.empty[E]
    if (value.charAt(idx) == '}') return set.toSet
    while (idx < value.length) {
      var n: Int = 0
      try {
        n = ParseUtils.skipCQLValue(value, idx)
      }
      catch {
        case e: IllegalArgumentException =>
          throw new InvalidTypeException( s"""Cannot parse set value from "$value", invalid CQL value at character $idx""", e)
      }
      set += eltCodec.parse(value.substring(idx, n))
      idx = n
      idx = ParseUtils.skipSpaces(value, idx)
      if (value.charAt(idx) == '}') return set.toSet
      if (value.charAt(idx) != ',') throw new InvalidTypeException( s"""Cannot parse set value from "$value", at character $idx expecting ',' but got '${value.charAt(idx)}'""")
      idx = ParseUtils.skipSpaces(value, idx + 1)
    }
    throw new InvalidTypeException( s"""Malformed set value "$value", missing closing '}'""")
  }

  override def accepts(value: AnyRef): Boolean = {
    value match {
      case set: Set[_] => if (set.isEmpty) true else eltCodec.accepts(set.head)
      case _ => false
    }
  }

}

object SetCodec {

  def apply[E](eltCodec: TypeCodec[E]) = {
    new SetCodec[E](DataType.set(eltCodec.getCqlType), TypeTokens.setOf(eltCodec.getJavaType), eltCodec)
  }

}
