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

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.policies.RetryPolicy;

import java.nio.ByteBuffer;
import java.util.Map;

public interface PreparedGraphStatement extends PreparedStatement, GraphStatement {

    String getGraphLanguage();

    String getGraphKeyspace();

    String getGraphSource();

    String getGraphRebinding();

    @Override
    BoundGraphStatement bind(Object... values);

    @Override
    BoundGraphStatement bind();

    @Override
    PreparedGraphStatement setRoutingKey(ByteBuffer routingKey);

    @Override
    PreparedGraphStatement setRoutingKey(ByteBuffer... routingKeyComponents);

    @Override
    PreparedGraphStatement setConsistencyLevel(ConsistencyLevel consistency);

    @Override
    PreparedGraphStatement setSerialConsistencyLevel(ConsistencyLevel serialConsistency);

    @Override
    PreparedGraphStatement enableTracing();

    @Override
    PreparedGraphStatement disableTracing();

    @Override
    PreparedGraphStatement setRetryPolicy(RetryPolicy policy);

    @Override
    PreparedGraphStatement setOutgoingPayload(Map<String, ByteBuffer> payload);

    @Override
    PreparedGraphStatement setIdempotent(Boolean idempotent);
}
