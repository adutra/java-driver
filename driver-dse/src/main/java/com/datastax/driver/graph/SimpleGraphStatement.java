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
    private volatile boolean dirty = false;

    protected SimpleGraphStatement(String query) {
        this.query = query;
        this.valuesMap = new HashMap<String, Object>();
    }

    @Override
    public String getQueryString() {
        return query;
    }

    @Override
    public String getQueryString(CodecRegistry codecRegistry) {
        return query;
    }

    @Override
    public ByteBuffer[] getValues(ProtocolVersion protocolVersion, CodecRegistry codecRegistry) {
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

                    if (value instanceof Integer) {
                        parameter.put("value", (Integer) value);
                    } else if (value instanceof String) {
                        parameter.put("value", (String) value);
                    } else if (value instanceof Float) {
                        parameter.put("value", (Float) value);
                    } else if (value instanceof Double) {
                        parameter.put("value", (Double) value);
                    } else if (value instanceof Boolean) {
                        parameter.put("value", (Boolean) value);
                    } else if (value instanceof Long) {
                        parameter.put("value", (Long) value);
                    } else {
                        throw new DriverException("Parameter : " + value + ", is not in a valid format to be sent as Gremlin parameter.");
                    }
                    parameter.put("name", name);
                    GraphUtils.OBJECT_MAPPER.writeTree(generator, parameter);
                    values.add(ByteBuffer.wrap(stringWriter.toString().getBytes(Charsets.UTF_8)));
                }
                this.values = values.toArray(new ByteBuffer[values.size()]);
                this.dirty = false;
            } catch (IOException e) {
                throw new DriverException("Some values are not in a compatible type to be serialized in a Gremlin Query.");
            }
        }
        return values;
    }

    @Override
    public boolean hasValues(CodecRegistry codecRegistry) {
        return !valuesMap.isEmpty();
    }

    /**
     * Set a parameter value for the statement.
     * <p/>
     * Values can be any type supported in JSON.
     *
     * @param name  Name of the value, defined in the query. Parameters in Gremlin are named as variables, no
     *              need for a CQL syntax like the bind marker "?" or the identifier ":" in front of a parameter.
     *              Please refer to Gremlin's documentation for more information.
     * @param value Any object serializable in JSON. The type will be detected automatically at statement's execution.
     */
    public void set(String name, Object value) {
        valuesMap.put(name, value);
        dirty = true;
    }
}
