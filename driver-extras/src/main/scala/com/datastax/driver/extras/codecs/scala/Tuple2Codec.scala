package com.datastax.driver.extras.codecs.scala

import java.nio.ByteBuffer
import java.util

import com.datastax.driver.core.TypeCodec.AbstractTupleCodec
import com.datastax.driver.core.{DataType, ProtocolVersion, TupleType, TypeCodec}
import com.google.common.reflect.TypeToken

class Tuple2Codec[T1, T2](cqlType: TupleType, javaType: TypeToken[(T1, T2)], eltCodecs: (TypeCodec[T1], TypeCodec[T2])) extends AbstractTupleCodec[(T1, T2)](cqlType, javaType) {

  override protected def newInstance(): (T1, T2) = null

  override protected def serializeField(source: (T1, T2), index: Int, protocolVersion: ProtocolVersion): ByteBuffer = {
    index match {
      case 0 => eltCodecs._1.serialize(source._1, protocolVersion)
      case 1 => eltCodecs._2.serialize(source._2, protocolVersion)
    }
  }

  override protected def deserializeAndSetField(input: ByteBuffer, target: (T1, T2), index: Int, protocolVersion: ProtocolVersion): (T1, T2) = {
    index match {
      case 0 => Tuple2(eltCodecs._1.deserialize(input, protocolVersion), null.asInstanceOf[T2])
      case 1 => target.copy(_2 = eltCodecs._2.deserialize(input, protocolVersion))
    }
  }

  override protected def formatField(source: (T1, T2), index: Int): String = {
    index match {
      case 0 => eltCodecs._1.format(source._1)
      case 1 => eltCodecs._2.format(source._2)
    }
  }

  override protected def parseAndSetField(input: String, target: (T1, T2), index: Int): (T1, T2) = {
    index match {
      case 0 => Tuple2(eltCodecs._1.parse(input), null.asInstanceOf[T2])
      case 1 => target.copy(_2 = eltCodecs._2.parse(input))
    }
  }

}

object Tuple2Codec {

  def apply[T1, T2](cqlType: TupleType, eltCodec1: TypeCodec[T1], eltCodec2: TypeCodec[T2]) = {
    val componentTypes: util.List[DataType] = cqlType.getComponentTypes
    if (componentTypes.size() != 2)
      throw new IllegalArgumentException("Expecting TupleType with 2 components, got " + componentTypes.size())
    if (!eltCodec1.accepts(componentTypes.get(0)))
      throw new IllegalArgumentException("Codec for component 1 does not accept component type: " + componentTypes.get(0))
    if (!eltCodec2.accepts(componentTypes.get(1)))
      throw new IllegalArgumentException("Codec for component 2 does not accept component type: " + componentTypes.get(1))
    new Tuple2Codec[T1, T2](cqlType, TypeTokens.tuple2Of(eltCodec1.getJavaType, eltCodec2.getJavaType), Tuple2(eltCodec1, eltCodec2))
  }

}

