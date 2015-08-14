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
package com.datastax.driver.core;

import java.util.Arrays;
import java.util.List;

import com.google.common.collect.ImmutableList;

/**
 * A tuple type.
 * <p>
 * A tuple type is a essentially a list of types.
 */
public class TupleType extends DataType {

    private final List<DataType> types;

    TupleType(List<DataType> types) {
        super(DataType.Name.TUPLE);
        this.types = ImmutableList.copyOf(types);
    }

    /**
     * Creates a tuple type given a list of types.
     *
     * @param types the types for the tuple type.
     * @return the newly created tuple type.
     */
    public static TupleType of(DataType... types) {
        return new TupleType(Arrays.asList(types));
    }

    /**
     * The (immutable) list of types composing this tuple type.
     *
     * @return the (immutable) list of types composing this tuple type.
     */
    public List<DataType> getComponentTypes() {
        return types;
    }

    /**
     * Returns a new empty value for this tuple type.
     *
     * @param protocolVersion The protocol version to use.
     * @param codecRegistry The {@link CodecRegistry} instance to use.
     * @return an empty (with all component to {@code null}) value for this
     * tuple definition.
     */
    public TupleValue newValue(ProtocolVersion protocolVersion, CodecRegistry codecRegistry) {
        return new TupleValue(this, protocolVersion, codecRegistry);
    }

    @Override
    public boolean isFrozen() {
        return true;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(new Object[]{ name, types });
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof TupleType))
            return false;

        TupleType d = (TupleType)o;
        return name == d.name && types.equals(d.types);
    }

    /**
     * Return {@code true} if this tuple type contains the given tuple type,
     * and {@code false} otherwise.
     * <p>
     * A tuple type is said to contain another one
     * if the latter has fewer components than the former,
     * but all of them are of the same type.
     * E.g. the type {@code tuple<int, text>}
     * is contained by the type {@code tuple<int, text, double>}.
     * <p>
     * A contained type can be seen as a "partial" view
     * of a containing type, where the missing components
     * are supposed to be {@code null}.
     *
     * @param other the tuple type to compare against the current one
     * @return {@code true} if this tuple type contains the given tuple type,
     * and {@code false} otherwise.
     */
    public boolean contains(TupleType other) {
        if(this.equals(other))
            return true;
        if(other.types.size() > this.types.size())
            return false;
        return types.subList(0, other.types.size()).equals(other.types);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (DataType type : types) {
            sb.append(sb.length() == 0 ? "frozen<tuple<" : ", ");
            sb.append(type);
        }
        return sb.append(">>").toString();
    }
}
