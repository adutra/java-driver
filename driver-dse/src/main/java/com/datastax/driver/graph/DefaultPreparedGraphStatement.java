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
package com.datastax.driver.graph;

import com.datastax.driver.core.PreparedId;
import com.datastax.driver.core.PreparedStatement;

import java.nio.ByteBuffer;
import java.util.Map;

public class DefaultPreparedGraphStatement implements PreparedGraphStatement {

    private final PreparedStatement wrapped;

    private final GraphOptions graphOptions;

    public DefaultPreparedGraphStatement(PreparedStatement wrapped, GraphOptions graphOptions) {
        this.wrapped = wrapped;
        this.graphOptions = graphOptions;
    }

    @Override
    public BoundGraphStatement bind(Object... values) {
        return new BoundGraphStatement(wrapped.bind(values));
    }

    @Override
    public BoundGraphStatement bind() {
        return new BoundGraphStatement(wrapped.bind());
    }


    public String getQueryString() {
        return wrapped.getQueryString();
    }


    @Override
    public PreparedId getPreparedId() {
        return wrapped.getPreparedId();
    }

    @Override
    public Map<String, ByteBuffer> getIncomingPayload() {
        return wrapped.getIncomingPayload();
    }

    @Override
    public Map<String, ByteBuffer> getOutgoingPayload() {
        return wrapped.getOutgoingPayload();
    }

    @Override
    public PreparedGraphStatement setOutgoingPayload(Map<String, ByteBuffer> payload) {
        wrapped.setOutgoingPayload(payload);
        return this;
    }

}
