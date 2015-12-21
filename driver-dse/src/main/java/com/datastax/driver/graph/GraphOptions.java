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

import com.datastax.driver.core.QueryOptions;
import com.google.common.collect.ImmutableMap;

import java.nio.ByteBuffer;
import java.util.Map;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Preconditions.checkNotNull;

public class GraphOptions extends QueryOptions {

    // Static keys for the Custom Payload map
    private static final String GRAPH_SOURCE_KEY = "graph-source";
    private static final String GRAPH_KEYSPACE_KEY = "graph-keyspace";
    private static final String GRAPH_LANGUAGE_KEY = "graph-language";
    private static final String GRAPH_REBINDING_KEY = "graph-rebinding";


    private static final String DEFAULT_GRAPH_LANGUAGE = "gremlin-groovy";
    private static final String DEFAULT_GRAPH_SOURCE = "default";

    private volatile String graphLanguage = DEFAULT_GRAPH_LANGUAGE;
    private volatile String graphSource = DEFAULT_GRAPH_SOURCE;
    private volatile String graphKeyspace;
    private volatile String graphRebinding;

    public GraphOptions() {
    }

    private GraphOptions(String graphLanguage, String graphSource, String graphKeyspace, String graphRebinding) {
        this.graphLanguage = graphLanguage;
        this.graphSource = graphSource;
        this.graphKeyspace = graphKeyspace;
        this.graphRebinding = graphRebinding;
    }

    public String getGraphLanguage() {
        return graphLanguage;
    }

    /**
     * Set the default Graph language to use in the query.
     * <p/>
     * The default value for this property is "gremlin-groovy".
     *
     * @param graphLanguage The language used in queries.
     * @return This {@link GraphOptions} instance to chain call.
     */
    public GraphOptions setGraphLanguage(String graphLanguage) {
        checkNotNull(graphLanguage, "graphLanguage cannot be null");
        this.graphLanguage = graphLanguage;
        return this;
    }

    public String getGraphSource() {
        return graphSource;
    }

    /**
     * Set the default Graph traversal source name on the graph side.
     * <p/>
     * The default value for this property is "default".
     *
     * @param graphSource The graph traversal source's name to use.
     * @return This {@link GraphOptions} instance to chain call.
     */
    public GraphOptions setGraphSource(String graphSource) {
        checkNotNull(graphSource, "graphSource cannot be null");
        this.graphSource = graphSource;
        return this;
    }

    public String getGraphKeyspace() {
        return graphKeyspace;
    }

    /**
     * Set the default Cassandra keyspace name storing the graph.
     *
     * @param graphKeyspace The Cassandra keyspace name to use.
     * @return This {@link GraphOptions} instance to chain call.
     */
    public GraphOptions setGraphKeyspace(String graphKeyspace) {
        checkNotNull(graphSource, "graphKeyspace cannot be null");
        this.graphKeyspace = graphKeyspace;
        return this;
    }

    public String getGraphRebinding() {
        return graphRebinding;
    }

    /**
     * Set the default Graph rebinding name to use.
     * <p/>
     * The default value for this property is "default".
     *
     * @param graphRebinding The graph traversal source's name to use.
     * @return This {@link GraphOptions} instance to chain call.
     */
    public GraphOptions setGraphRebinding(String graphRebinding) {
        this.graphRebinding = graphRebinding;
        return this;
    }

    public Map<String, ByteBuffer> asPayload() {
        ImmutableMap.Builder<String, ByteBuffer> payload = ImmutableMap.builder();
        if (graphLanguage != null)
            payload.put(GRAPH_LANGUAGE_KEY, ByteBuffer.wrap(graphLanguage.getBytes(UTF_8)));
        if (graphKeyspace != null)
            payload.put(GRAPH_KEYSPACE_KEY, ByteBuffer.wrap(graphKeyspace.getBytes(UTF_8)));
        if (graphSource != null)
            payload.put(GRAPH_SOURCE_KEY, ByteBuffer.wrap(graphSource.getBytes(UTF_8)));
        if (graphRebinding != null)
            payload.put(GRAPH_REBINDING_KEY, ByteBuffer.wrap(graphRebinding.getBytes(UTF_8)));
        return payload.build();
    }

    public void merge(GraphOptions defaultGraphOptions) {
        if (getGraphLanguage() == null)
            setGraphLanguage(defaultGraphOptions.getGraphLanguage());
        if (getGraphKeyspace() == null)
            setGraphKeyspace(defaultGraphOptions.getGraphKeyspace());
        if (getGraphSource() == null)
            setGraphSource(defaultGraphOptions.getGraphSource());
        if (getGraphRebinding() == null)
            setGraphRebinding(defaultGraphOptions.getGraphRebinding());
    }
}
