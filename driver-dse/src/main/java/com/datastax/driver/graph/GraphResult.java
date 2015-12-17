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

import com.datastax.driver.core.exceptions.DriverException;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.shaded.jackson.databind.JsonNode;

import java.io.IOException;

/**
 * A result entity containing a graph query's result, wrapping a Json result.
 */
public class GraphResult {

    private final JsonNode jsonNode;

    GraphResult(JsonNode jsonNode) {
        this.jsonNode = jsonNode;
    }

    /**
     * Get the raw enclosed JSON object.
     *
     * @return The enclosed JSON object.
     */
    public JsonNode getJsonObject() {
        return this.jsonNode;
    }

    /**
     * Return the encapsulated result as a String.
     *
     * @return A String of the encapsulated result.
     */
    public String asString() {
        if (this.jsonNode != null)
            return this.jsonNode.asText();
        return null;
    }

    /**
     * Return the encapsulated result as an integer.
     *
     * @return An integer of the encapsulated result.
     */
    public Integer asInt() {
        if (this.jsonNode != null)
            return this.jsonNode.asInt();
        return null;
    }

    /**
     * Return the encapsulated result as a boolean.
     *
     * @return A boolean of the encapsulated result.
     */
    public Boolean asBool() {
        if (this.jsonNode != null)
            return this.jsonNode.asBoolean();
        return null;
    }

    /**
     * Return the encapsulated result as a long integer.
     *
     * @return A long integer of the encapsulated result.
     */
    public Long asLong() {
        if (this.jsonNode != null)
            return this.jsonNode.asLong();
        return null;
    }

    /**
     * Return the encapsulated result as a double.
     *
     * @return A double of the encapsulated result.
     */
    public Double asDouble() {
        if (this.jsonNode != null)
            return this.jsonNode.asDouble();
        return null;
    }

    public Vertex asVertex() {
        try {
            if (this.jsonNode != null) {
                return GraphUtils.asVertex(jsonNode);
            }
            return null;
        } catch (IOException e) {
            throw new DriverException(e);
        }
    }

    public Edge asEdge() {
        try {
            if (this.jsonNode != null) {
                return GraphUtils.asEdge(jsonNode);
            }
            return null;
        } catch (IOException e) {
            throw new DriverException(e);
        }
    }

    public Property asProperty() {
        try {
            if (this.jsonNode != null) {
                return GraphUtils.asProperty(jsonNode);
            }
            return null;
        } catch (IOException e) {
            throw new DriverException(e);
        }
    }

    public VertexProperty asVertexProperty() {
        try {
            if (this.jsonNode != null) {
                return GraphUtils.asVertexProperty(jsonNode);
            }
            return null;
        } catch (IOException e) {
            throw new DriverException(e);
        }
    }

    @Override
    public String toString() {
        return this.jsonNode != null
                ? this.jsonNode.toString()
                : null;
    }
}

