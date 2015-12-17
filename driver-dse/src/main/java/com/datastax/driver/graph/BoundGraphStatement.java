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

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.ProtocolVersion;

import java.nio.ByteBuffer;

/**
 * Bound graph statement produced from a prepared statement.
 */
public class BoundGraphStatement extends BoundStatement implements GraphStatement {

    private final BoundStatement wrapped;

    private final PreparedGraphStatement preparedGraphStatement;

    BoundGraphStatement(BoundStatement wrapped) {
        super(wrapped.preparedStatement());
        this.wrapped = wrapped;
        this.preparedGraphStatement = (PreparedGraphStatement)wrapped.preparedStatement();
    }


    @Override
    public ByteBuffer getRoutingKey(ProtocolVersion protocolVersion, CodecRegistry codecRegistry) {
        // as per javadoc: implementors are free to return null
        return null;
    }

    @Override
    public String getKeyspace() {
        // as per javadoc: implementors are free to return null
        return null;
    }

    @Override
    public String getGraphSource() {
        return preparedGraphStatement.getGraphSource();
    }

    @Override
    public String getGraphLanguage() {
        return preparedGraphStatement.getGraphLanguage();
    }

    @Override
    public String getGraphKeyspace() {
        return preparedGraphStatement.getGraphKeyspace();
    }

    @Override
    public String getGraphRebinding() {
        return preparedGraphStatement.getGraphRebinding();
    }

    @Override
    public BoundGraphStatement unwrap() {
        return this;
    }

    // TODO

}
