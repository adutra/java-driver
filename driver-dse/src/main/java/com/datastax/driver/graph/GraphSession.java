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

import com.datastax.driver.core.Session;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * The GraphSession interface allows to execute and prepare graph specific statements.
 * <p/>
 * You should use this object whenever the statement to execute is a graph query.
 * The object will make verifications before executing queries to make sure the
 * statement sent is valid.
 * This object also generates graph result sets.
 */
public interface GraphSession extends Session {

    GraphResultSet executeGraph(String query);

    GraphResultSet executeGraph(GraphStatement statement);

    ListenableFuture<GraphResultSet> executeGraphAsync(String query);

    ListenableFuture<GraphResultSet> executeGraphAsync(GraphStatement statement);

    PreparedGraphStatement prepareGraph(String query);

    PreparedGraphStatement prepareGraph(RegularGraphStatement statement);

    ListenableFuture<PreparedGraphStatement> prepareGraphAsync(String query);

    ListenableFuture<PreparedGraphStatement> prepareGraphAsync(RegularGraphStatement statement);

}
