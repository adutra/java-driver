package com.datastax.driver.extras.codecs.scala

import java.nio.ByteBuffer
import java.util

import com.datastax.driver.core.TypeCodec.AbstractTupleCodec
import com.datastax.driver.core.{DataType, ProtocolVersion, TupleType, TypeCodec}
import com.google.common.reflect.TypeToken

class Tuple3Codec[T1, T2, T3](cqlType: TupleType, javaType: TypeToken[(T1, T2, T3)], eltCodecs: (TypeCodec[T1], TypeCodec[T2], TypeCodec[T3])) extends AbstractTupleCodec[(T1, T2, T3)](cqlType, javaType) {

  override protected def newInstance(): (T1, T2, T3) = null

  override protected def serializeField(source: (T1, T2, T3), index: Int, protocolVersion: ProtocolVersion): ByteBuffer = {
    index match {
      case 0 => eltCodecs._1.serialize(source._1, protocolVersion)
      case 1 => eltCodecs._2.serialize(source._2, protocolVersion)
      case 2 => eltCodecs._3.serialize(source._3, protocolVersion)
    }
  }

  override protected def deserializeAndSetField(input: ByteBuffer, target: (T1, T2, T3), index: Int, protocolVersion: ProtocolVersion): (T1, T2, T3) = {
    index match {
      case 0 => Tuple3(eltCodecs._1.deserialize(input, protocolVersion), null.asInstanceOf[T2], null.asInstanceOf[T3])
      case 1 => target.copy(_2 = eltCodecs._2.deserialize(input, protocolVersion))
      case 2 => target.copy(_3 = eltCodecs._3.deserialize(input, protocolVersion))
    }
  }

  override protected def formatField(source: (T1, T2, T3), index: Int): String = {
    index match {
      case 0 => eltCodecs._1.format(source._1)
      case 1 => eltCodecs._2.format(source._2)
      case 2 => eltCodecs._3.format(source._3)
    }
  }

  override protected def parseAndSetField(input: String, target: (T1, T2, T3), index: Int): (T1, T2, T3) = {
    index match {
      case 0 => Tuple3(eltCodecs._1.parse(input), null.asInstanceOf[T2], null.asInstanceOf[T3])
      case 1 => target.copy(_2 = eltCodecs._2.parse(input))
      case 2 => target.copy(_3 = eltCodecs._3.parse(input))
    }
  }

}

object Tuple3Codec {

  def apply[T1, T2, T3](cqlType: TupleType, eltCodec1: TypeCodec[T1], eltCodec2: TypeCodec[T2], eltCodec3: TypeCodec[T3]) = {
    val componentTypes: util.List[DataType] = cqlType.getComponentTypes
    if (componentTypes.size() != 3)
      throw new IllegalArgumentException("Expecting TupleType with 3 components, got " + componentTypes.size())
    if (!eltCodec1.accepts(componentTypes.get(0)))
      throw new IllegalArgumentException("Codec for component 1 does not accept component type: " + componentTypes.get(0))
    if (!eltCodec2.accepts(componentTypes.get(1)))
      throw new IllegalArgumentException("Codec for component 2 does not accept component type: " + componentTypes.get(1))
    if (!eltCodec3.accepts(componentTypes.get(2)))
      throw new IllegalArgumentException("Codec for component 3 does not accept component type: " + componentTypes.get(2))
    new Tuple3Codec[T1, T2, T3](cqlType, TypeTokens.tuple3Of(eltCodec1.getJavaType, eltCodec2.getJavaType, eltCodec3.getJavaType), Tuple3(eltCodec1, eltCodec2, eltCodec3))
  }

}

