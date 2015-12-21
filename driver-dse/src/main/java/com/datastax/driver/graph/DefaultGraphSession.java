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
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Uninterruptibles;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class DefaultGraphSession extends AbstractSession implements GraphSession {

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
        statement.getGraphOptions().merge(defaultGraphOptions());
        ResultSetFuture resultSetFuture = wrapped.executeAsync(statement.unwrap());
        return Futures.transform(resultSetFuture, new Function<ResultSet, GraphResultSet>() {
            @Override
            public GraphResultSet apply(ResultSet input) {
                return new GraphResultSet(input);
            }
        });
    }

    @Override
    public PreparedGraphStatement prepareGraph(String query) {
        return prepareGraph(new SimpleGraphStatement(query));
    }

    @Override
    public PreparedGraphStatement prepareGraph(RegularGraphStatement statement) {
        try {
            return Uninterruptibles.getUninterruptibly(prepareGraphAsync(statement));
        } catch (ExecutionException e) {
            throw DriverThrowables.propagateCause(e);
        }
    }

    @Override
    public ListenableFuture<PreparedGraphStatement> prepareGraphAsync(String query) {
        return prepareGraphAsync(new SimpleGraphStatement(query));
    }

    @Override
    public ListenableFuture<PreparedGraphStatement> prepareGraphAsync(final RegularGraphStatement statement) {
        statement.getGraphOptions().merge(defaultGraphOptions());
        ListenableFuture<PreparedStatement> future = wrapped.prepareAsync(statement.unwrap());
        return Futures.transform(future, new Function<PreparedStatement, PreparedGraphStatement>() {
            @Override
            public PreparedGraphStatement apply(PreparedStatement input) {
                return new DefaultPreparedGraphStatement(input, statement.getGraphOptions());
            }
        });
    }

    private GraphOptions defaultGraphOptions() {
        GraphOptions defaultGraphOptions;
        if (getCluster().getConfiguration().getQueryOptions() instanceof GraphOptions)
            defaultGraphOptions = ((GraphOptions) getCluster().getConfiguration().getQueryOptions());
        else
            defaultGraphOptions = new GraphOptions();
        return defaultGraphOptions;
    }
}
