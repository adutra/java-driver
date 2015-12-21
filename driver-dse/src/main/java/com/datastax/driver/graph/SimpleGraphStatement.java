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

import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.exceptions.DriverException;
import com.google.common.base.Charsets;
import org.apache.tinkerpop.shaded.jackson.core.JsonFactory;
import org.apache.tinkerpop.shaded.jackson.core.JsonGenerator;
import org.apache.tinkerpop.shaded.jackson.databind.node.JsonNodeFactory;
import org.apache.tinkerpop.shaded.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A simple graph statement implementation.
 * THis class is not thread-safe.
 */
public class SimpleGraphStatement extends RegularGraphStatement {

    private final String query;
    private final Map<String, Object> valuesMap;
    private ByteBuffer[] values;
    private boolean dirty = false;

    protected SimpleGraphStatement(String query) {
        this.query = query;
        this.valuesMap = new HashMap<String, Object>();
    }

    public String getQueryString() {
        return query;
    }

    public boolean hasValues() {
        return !valuesMap.isEmpty();
    }

    @Override
    public SimpleStatement unwrap() {
        SimpleStatement stmt = new SimpleStatement(query, values);
        stmt.setOutgoingPayload(getGraphOptions().asPayload());
        return stmt;
    }

    private void maybeRebuildValues() {
        if (dirty) {
            JsonNodeFactory factory = new JsonNodeFactory(false);
            JsonFactory jsonFactory = new JsonFactory();
            List<ByteBuffer> values = new ArrayList<ByteBuffer>();
            try {
                for (Map.Entry<String, Object> param : this.valuesMap.entrySet()) {
                    StringWriter stringWriter = new StringWriter();
                    JsonGenerator generator = jsonFactory.createGenerator(stringWriter);
                    ObjectNode parameter = factory.objectNode();
                    String name = param.getKey();
                    Object value = param.getValue();
                    parameter.put("name", name);
                    parameter.putPOJO("value", value);
                    GraphUtils.OBJECT_MAPPER.writeTree(generator, parameter);
                    values.add(ByteBuffer.wrap(stringWriter.toString().getBytes(Charsets.UTF_8)));
                }
                this.values = values.toArray(new ByteBuffer[values.size()]);
                this.dirty = false;
            } catch (IOException e) {
                throw new DriverException("Some values are not in a compatible type to be serialized in a Gremlin Query.");
            }
        }
    }

}
