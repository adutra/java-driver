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

import static com.google.common.base.Preconditions.checkNotNull;

public class GraphQueryOptions extends QueryOptions {

    private static final String DEFAULT_GRAPH_LANGUAGE = "gremlin-groovy";
    private static final String DEFAULT_GRAPH_SOURCE = "default";

    private volatile String graphLanguage = DEFAULT_GRAPH_LANGUAGE;
    private volatile String graphSource = DEFAULT_GRAPH_SOURCE;
    private volatile String graphKeyspace;
    private volatile String graphRebinding;

    public String getGraphLanguage() {
        return graphLanguage;
    }

    /**
     * Set the default Graph language to use in the query.
     * <p/>
     * The default value for this property is "gremlin-groovy".
     *
     * @param graphLanguage The language used in queries.
     * @return This {@link GraphQueryOptions} instance to chain call.
     */
    public GraphQueryOptions setGraphLanguage(String graphLanguage) {
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
     * @return This {@link GraphQueryOptions} instance to chain call.
     */
    public GraphQueryOptions setGraphSource(String graphSource) {
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
     * @return This {@link GraphQueryOptions} instance to chain call.
     */
    public GraphQueryOptions setGraphKeyspace(String graphKeyspace) {
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
     * @return This {@link GraphQueryOptions} instance to chain call.
     */
    public GraphQueryOptions setGraphRebinding(String graphRebinding) {
        this.graphRebinding = graphRebinding;
        return this;
    }
}
