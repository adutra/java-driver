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

import com.datastax.driver.core.PreparedStatement;

public class PreparedGraphStatement extends GraphStatement {

    private final PreparedStatement wrapped;

    private final GraphOptions graphOptions;

    public PreparedGraphStatement(PreparedStatement wrapped, GraphOptions graphOptions) {
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


}
