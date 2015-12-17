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

import com.datastax.driver.core.*;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Uninterruptibles;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Preconditions.checkNotNull;

public class DefaultGraphSession extends AbstractSession implements GraphSession {

    // Static keys for the Custom Payload map
    private static final String GRAPH_SOURCE_KEY = "graph-source";
    private static final String GRAPH_KEYSPACE_KEY = "graph-keyspace";
    private static final String GRAPH_LANGUAGE_KEY = "graph-language";
    private static final String GRAPH_REBINDING_KEY = "graph-rebinding";

    private final Session wrapped;

    /**
     * Create a DefaultGraphSession object that wraps an underlying Java Driver Session.
     * The Java Driver session has to be initialised.
     *
     * @param wrapped the session to wrap, basically the return of a cluster.connect() call.
     */
    public DefaultGraphSession(Session wrapped) {
        this.wrapped = wrapped;
    }

    @Override
    public String getLoggedKeyspace() {
        return wrapped.getLoggedKeyspace();
    }

    @Override
    public CloseFuture closeAsync() {
        return wrapped.closeAsync();
    }

    @Override
    public boolean isClosed() {
        return wrapped.isClosed();
    }

    @Override
    public Cluster getCluster() {
        return wrapped.getCluster();
    }

    @Override
    public State getState() {
        return wrapped.getState();
    }

    @Override
    public GraphSession init() {
        try {
            return (GraphSession)Uninterruptibles.getUninterruptibly(initAsync());
        } catch (ExecutionException e) {
            throw DriverThrowables.propagateCause(e);
        }
    }

    @Override
    public ListenableFuture<Session> initAsync() {
        return Futures.transform(wrapped.initAsync(), new Function<Session, Session>() {
            @Override
            public Session apply(Session input) {
                return new DefaultGraphSession(input);
            }
        });
    }

    @Override
    public ListenableFuture<PreparedStatement> prepareAsync(String query, Map<String, ByteBuffer> customPayload) {
        return wrapped.prepareAsync(query, customPayload);
    }

    @Override
    public ResultSetFuture executeAsync(Statement statement) {
        return wrapped.executeAsync(statement);
    }

    @Override
    public GraphResultSet executeGraph(String query) {
        return executeGraph(new SimpleGraphStatement(query));
    }

    @Override
    public GraphResultSet executeGraph(GraphStatement statement) {
        try {
            return Uninterruptibles.getUninterruptibly(executeGraphAsync(statement));
        } catch (ExecutionException e) {
            throw DriverThrowables.propagateCause(e);
        }
    }

    @Override
    public ListenableFuture<GraphResultSet> executeGraphAsync(String query) {
        return executeGraphAsync(new SimpleGraphStatement(query));
    }

    @Override
    public ListenableFuture<GraphResultSet> executeGraphAsync(GraphStatement statement) {
        if (statement instanceof RegularGraphStatement) {
            Map<String, ByteBuffer> payload = preparePayload((RegularGraphStatement) statement);
            statement.unwrap().setOutgoingPayload(payload);
        }
        ResultSetFuture resultSetFuture = wrapped.executeAsync(statement.unwrap());
        return Futures.transform(resultSetFuture, new Function<ResultSet, GraphResultSet>() {
            @Override
            public GraphResultSet apply(ResultSet input) {
                return new GraphResultSet(input);
            }
        });
    }

    private ImmutableMap<String, ByteBuffer> preparePayload(RegularGraphStatement statement) {
        GraphQueryOptions graphQueryOptions;
        if (getCluster().getConfiguration().getQueryOptions() instanceof GraphQueryOptions)
            graphQueryOptions = ((GraphQueryOptions) getCluster().getConfiguration().getQueryOptions());
        else
            graphQueryOptions = new GraphQueryOptions();

        String graphLanguage = statement.getGraphLanguage() == null ? graphQueryOptions.getGraphLanguage() : statement.getGraphLanguage();
        String graphKeyspace = statement.getGraphKeyspace() == null ? graphQueryOptions.getGraphKeyspace() : statement.getGraphKeyspace();
        String graphSource = statement.getGraphSource() == null ? graphQueryOptions.getGraphSource() : statement.getGraphSource();
        String graphRebinding = statement.getGraphRebinding() == null ? graphQueryOptions.getGraphRebinding() : statement.getGraphRebinding();

        checkNotNull(graphLanguage, "Graph queries require the graph-language custom payload key to be specified");
        checkNotNull(graphKeyspace, "Graph queries require the graph-keyspace custom payload key to be specified");
        checkNotNull(graphSource, "Graph queries require the graph-source custom payload key to be specified");

        ImmutableMap.Builder<String, ByteBuffer> payload = ImmutableMap.builder();

        // preserve any existing payload
        if (statement.unwrap().getOutgoingPayload() != null)
            payload.putAll(statement.unwrap().getOutgoingPayload());

        // and add/ override graph-specific keys
        payload
                .put(GRAPH_LANGUAGE_KEY, ByteBuffer.wrap(graphLanguage.getBytes(UTF_8)))
                .put(GRAPH_KEYSPACE_KEY, ByteBuffer.wrap(graphKeyspace.getBytes(UTF_8)))
                .put(GRAPH_SOURCE_KEY, ByteBuffer.wrap(graphKeyspace.getBytes(UTF_8)));

        if (graphRebinding != null)
            payload.put(GRAPH_REBINDING_KEY, ByteBuffer.wrap(graphRebinding.getBytes(UTF_8)));

        return payload.build();
    }
}
