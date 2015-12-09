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

import com.datastax.driver.core.CodecUtils.{readSize, readValue}
import com.datastax.driver.core.DataType.CollectionType
import com.datastax.driver.core._
import com.datastax.driver.core.exceptions.InvalidTypeException
import com.google.common.reflect.TypeToken

import scala.collection.mutable.ArrayBuffer

class SeqCodec[E](cqlType: CollectionType, javaType: TypeToken[Seq[E]], eltCodec: TypeCodec[E]) extends TypeCodec[Seq[E]](cqlType, javaType) {

  override def serialize(value: Seq[E], protocolVersion: ProtocolVersion): ByteBuffer = {
    if (value == null) return null
    val bbs: Seq[ByteBuffer] = for (elt <- value) yield {
      if (elt == null)
        throw new NullPointerException("List elements cannot be null")
      try {
        eltCodec.serialize(elt, protocolVersion)
      }
      catch {
        case e: ClassCastException =>
          throw new InvalidTypeException(s"Invalid type for list element, expecting ${eltCodec.getJavaType} but got ${elt.getClass}", e)
      }
    }
    CodecUtils.pack(bbs.toArray, value.size, protocolVersion)
  }

  override def deserialize(bytes: ByteBuffer, protocolVersion: ProtocolVersion): Seq[E] = {
    if (bytes == null || bytes.remaining == 0) return Seq.empty[E]
    try {
      val input: ByteBuffer = bytes.duplicate
      val size: Int = readSize(input, protocolVersion)
      for (_ <- 1 to size) yield eltCodec.deserialize(readValue(input, protocolVersion), protocolVersion)
    }
    catch {
      case e: BufferUnderflowException =>
        throw new InvalidTypeException("Not enough bytes to deserialize list", e)
    }
  }

  override def format(value: Seq[E]): String = {
    if (value == null) "NULL" else '[' + value.map(e => eltCodec.format(e)).mkString(",") + ']'
  }

  override def parse(value: String): Seq[E] = {
    if (value == null || value.isEmpty || value.equalsIgnoreCase("NULL")) return Seq.empty[E]
    var idx: Int = ParseUtils.skipSpaces(value, 0)
    if (value.charAt(idx) != '[') throw new InvalidTypeException( s"""Cannot parse list value from "$value", at character $idx expecting '[' but got '${value.charAt(idx)}'""")
    idx = ParseUtils.skipSpaces(value, idx + 1)
    val seq: ArrayBuffer[E] = ArrayBuffer.empty[E]
    if (value.charAt(idx) == ']') return seq.toSeq
    while (idx < value.length) {
      var n: Int = 0
      try {
        n = ParseUtils.skipCQLValue(value, idx)
      }
      catch {
        case e: IllegalArgumentException =>
          throw new InvalidTypeException( s"""Cannot parse list value from "$value", invalid CQL value at character $idx""", e)
      }
      seq += eltCodec.parse(value.substring(idx, n))
      idx = n
      idx = ParseUtils.skipSpaces(value, idx)
      if (value.charAt(idx) == ']') return seq.toSeq
      if (value.charAt(idx) != ',') throw new InvalidTypeException( s"""Cannot parse list value from "$value", at character $idx expecting ',' but got '${value.charAt(idx)}'""")
      idx = ParseUtils.skipSpaces(value, idx + 1)
    }
    throw new InvalidTypeException( s"""Malformed list value "$value", missing closing ']'""")
  }

  override def accepts(value: AnyRef): Boolean = {
    value match {
      case seq: Seq[_] => if (seq.isEmpty) true else eltCodec.accepts(seq.head)
      case _ => false
    }
  }

}

object SeqCodec {

  def apply[E](eltCodec: TypeCodec[E]) = {
    new SeqCodec[E](DataType.list(eltCodec.getCqlType), TypeTokens.seqOf(eltCodec.getJavaType), eltCodec)
  }

}
