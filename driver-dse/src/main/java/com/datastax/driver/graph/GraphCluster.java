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

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.DelegatingCluster;
import com.datastax.driver.core.Session;
import com.google.common.base.Function;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

public class GraphCluster extends DelegatingCluster {

    private final Cluster wrapped;

    public GraphCluster(Cluster wrapped) {
        this.wrapped = wrapped;
    }

    @Override
    protected Cluster delegate() {
        return wrapped;
    }

    @Override
    public GraphSession newSession() {
        return new DefaultGraphSession(super.newSession());
    }

    @Override
    public GraphSession connect() {
        return new DefaultGraphSession(super.connect());
    }

    @Override
    public GraphSession connect(String keyspace) {
        return new DefaultGraphSession(super.connect(keyspace));
    }

    @Override
    public ListenableFuture<Session> connectAsync() {
        return Futures.transform(super.connectAsync(), new Function<Session, Session>() {
            @Override
            public Session apply(Session input) {
                return new DefaultGraphSession(input);
            }
        });
    }

    @Override
    public ListenableFuture<Session> connectAsync(String keyspace) {
        return Futures.transform(super.connectAsync(keyspace), new Function<Session, Session>() {
            @Override
            public Session apply(Session input) {
                return new DefaultGraphSession(input);
            }
        });
    }
}
