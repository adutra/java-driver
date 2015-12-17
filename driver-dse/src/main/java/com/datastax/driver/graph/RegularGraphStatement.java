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

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.exceptions.InvalidQueryException;

import java.nio.ByteBuffer;

public abstract class RegularGraphStatement extends RegularStatement implements GraphStatement {

    private String graphLanguage;
    private String graphKeyspace;
    private String graphSource;
    private String graphRebinding;

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
    public RegularGraphStatement unwrap() {
        return this;
    }

    @Override
    public String getGraphLanguage() {
        return graphLanguage;
    }

    /**
     * Set the Graph language to use for this statement.
     *
     * @param graphLanguage the Graph language to use for this statement.
     * @return This {@link RegularGraphStatement} instance to allow chaining call.
     */
    public GraphStatement setGraphLanguage(String graphLanguage) {
        this.graphLanguage = graphLanguage;
        return this;
    }

    @Override
    public String getGraphKeyspace() {
        return graphKeyspace;
    }

    /**
     * Set the Graph keyspace to use for this statement.
     *
     * @param graphKeyspace the Graph keyspace to use for this statement. Throws an exception if the parameter is null or empty since this parameter is mandatory.
     * @return This {@link RegularGraphStatement} instance to allow chaining call.
     */
    public GraphStatement setGraphKeyspace(String graphKeyspace) {
        if (graphKeyspace == null || graphKeyspace.isEmpty()) {
            throw new InvalidQueryException("You cannot set null value or empty string to the keyspace for the Graph, this field is mandatory.");
        }
        this.graphKeyspace = graphKeyspace;
        return this;
    }

    @Override
    public String getGraphSource() {
        return graphSource;
    }

    /**
     * Set the Graph traversal source to use for this statement.
     *
     * @param graphSource the Graph traversal source to use for this statement.
     * @return This {@link RegularGraphStatement} instance to allow chaining call.
     */
    public GraphStatement setGraphSource(String graphSource) {
        this.graphSource = graphSource;
        return this;
    }

    @Override
    public String getGraphRebinding() {
        return graphRebinding;
    }

    /**
     * Set the Graph rebinding name to use for this statement.
     *
     * @param graphRebinding the Graph rebinding name to use for this statement.
     * @return This {@link RegularGraphStatement} instance to allow chaining call.
     */
    public GraphStatement setGraphRebinding(String graphRebinding) {
        this.graphRebinding = graphRebinding;
        return this;
    }

}

