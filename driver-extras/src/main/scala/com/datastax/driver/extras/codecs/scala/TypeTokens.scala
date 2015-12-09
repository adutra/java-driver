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

import com.google.common.reflect.{TypeParameter, TypeToken}

/**
  * Utility methods to create TypeToken instances.
  */
object TypeTokens {

  def seqOf[T](eltType: Class[T]): TypeToken[Seq[T]] = {
    // @formatter:off
    new TypeToken[Seq[T]]() {}.where(new TypeParameter[T]() {}, eltType)
    // @formatter:on
  }

  def seqOf[T](eltType: TypeToken[T]): TypeToken[Seq[T]] = {
    // @formatter:off
    new TypeToken[Seq[T]]() {}.where(new TypeParameter[T]() {}, eltType)
    // @formatter:on
  }

  def setOf[T](eltType: Class[T]): TypeToken[Set[T]] = {
    // @formatter:off
    new TypeToken[Set[T]]() {}.where(new TypeParameter[T]() {}, eltType)
    // @formatter:on
  }

  def setOf[T](eltType: TypeToken[T]): TypeToken[Set[T]] = {
    // @formatter:off
    new TypeToken[Set[T]]() {}.where(new TypeParameter[T]() {}, eltType)
    // @formatter:on
  }

  def mapOf[K, V](keyType: Class[K], valueType: Class[V]): TypeToken[Map[K, V]] = {
    // @formatter:off
    new TypeToken[Map[K, V]]() {}
      .where(new TypeParameter[K]() {}, keyType)
      .where(new TypeParameter[V]() {}, valueType)
    // @formatter:on
  }

  def mapOf[K, V](keyType: TypeToken[K], valueType: TypeToken[V]): TypeToken[Map[K, V]] = {
    // @formatter:off
    new TypeToken[Map[K, V]]() {}
      .where(new TypeParameter[K]() {}, keyType)
      .where(new TypeParameter[V]() {}, valueType)
    // @formatter:on
  }

  def optionOf[T](eltType: Class[T]): TypeToken[Option[T]] = {
    // @formatter:off
    new TypeToken[Option[T]]() {}.where(new TypeParameter[T]() {}, eltType)
    // @formatter:on
  }

  def optionOf[T](eltType: TypeToken[T]): TypeToken[Option[T]] = {
    // @formatter:off
    new TypeToken[Option[T]]() {}.where(new TypeParameter[T]() {}, eltType)
    // @formatter:on
  }

  def tuple2Of[T1, T2](type1: Class[T1], type2: Class[T2]): TypeToken[(T1, T2)] = {
    // @formatter:off
    new TypeToken[(T1, T2)]() {}
      .where(new TypeParameter[T1]() {}, type1)
      .where(new TypeParameter[T2]() {}, type2)
    // @formatter:on
  }

  def tuple2Of[T1, T2](type1: TypeToken[T1], type2: TypeToken[T2]): TypeToken[(T1, T2)] = {
    // @formatter:off
    new TypeToken[(T1, T2)]() {}
      .where(new TypeParameter[T1]() {}, type1)
      .where(new TypeParameter[T2]() {}, type2)
    // @formatter:on
  }

  def tuple3Of[T1, T2, T3](type1: Class[T1], type2: Class[T2], type3: Class[T3]): TypeToken[(T1, T2, T3)] = {
    // @formatter:off
    new TypeToken[(T1, T2, T3)]() {}
      .where(new TypeParameter[T1]() {}, type1)
      .where(new TypeParameter[T2]() {}, type2)
      .where(new TypeParameter[T3]() {}, type3)
    // @formatter:on
  }

  def tuple3Of[T1, T2, T3](type1: TypeToken[T1], type2: TypeToken[T2], type3: TypeToken[T3]): TypeToken[(T1, T2, T3)] = {
    // @formatter:off
    new TypeToken[(T1, T2, T3)]() {}
      .where(new TypeParameter[T1]() {}, type1)
      .where(new TypeParameter[T2]() {}, type2)
      .where(new TypeParameter[T3]() {}, type3)
    // @formatter:on
  }


}
