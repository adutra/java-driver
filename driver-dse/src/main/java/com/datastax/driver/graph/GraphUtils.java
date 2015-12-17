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

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONMapper;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONReader;
import org.apache.tinkerpop.gremlin.structure.util.Attachable;
import org.apache.tinkerpop.shaded.jackson.databind.JsonNode;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.function.Function;

import static com.google.common.base.Charsets.UTF_8;

class GraphUtils {

    static final GraphSONMapper GRAPHSON_MAPPER = GraphSONMapper.build()
//            .embedTypes(true)
            .loadCustomModules(true)
            .create();

    static final GraphSONReader GRAPHSON_READER = GraphSONReader.build().mapper(GRAPHSON_MAPPER).create();

    static final ObjectMapper OBJECT_MAPPER = GRAPHSON_MAPPER.createMapper();

    static final Function<Attachable<Vertex>, Vertex> VERTEX_ATTACH_METHOD = new Function<Attachable<Vertex>, Vertex>() {
        @Override
        public Vertex apply(Attachable<Vertex> vertexAttachable) {
            return vertexAttachable.get();
        }
    };

    static final Function<Attachable<Edge>, Edge> EDGE_ATTACH_METHOD = new Function<Attachable<Edge>, Edge>() {
        @Override
        public Edge apply(Attachable<Edge> edgeAttachable) {
            return edgeAttachable.get();
        }
    };

    static final Function<Attachable<Property>, Property> PROPERTY_ATTACH_METHOD = new Function<Attachable<Property>, Property>() {
        @Override
        public Property apply(Attachable<Property> propertyAttachable) {
            return propertyAttachable.get();
        }
    };

    static final Function<Attachable<VertexProperty>, VertexProperty> VERTEX_PROPERTY_ATTACH_METHOD = new Function<Attachable<VertexProperty>, VertexProperty>() {
        @Override
        public VertexProperty apply(Attachable<VertexProperty> vertexPropertyAttachable) {
            return vertexPropertyAttachable.get();
        }
    };

    static Vertex asVertex(JsonNode jsonNode) throws IOException {
        InputStream stream = new ByteArrayInputStream(jsonNode.toString().getBytes(UTF_8));
        return GRAPHSON_READER.readVertex(stream, VERTEX_ATTACH_METHOD);
    }

    static Edge asEdge(JsonNode jsonNode) throws IOException {
        InputStream stream = new ByteArrayInputStream(jsonNode.toString().getBytes(UTF_8));
        return GRAPHSON_READER.readEdge(stream, EDGE_ATTACH_METHOD);
    }

    static Property<?> asProperty(JsonNode jsonNode) throws IOException {
        InputStream stream = new ByteArrayInputStream(jsonNode.toString().getBytes(UTF_8));
        return GRAPHSON_READER.readProperty(stream, PROPERTY_ATTACH_METHOD);
    }

    static VertexProperty<?> asVertexProperty(JsonNode jsonNode) throws IOException {
        InputStream stream = new ByteArrayInputStream(jsonNode.toString().getBytes(UTF_8));
        return GRAPHSON_READER.readVertexProperty(stream, VERTEX_PROPERTY_ATTACH_METHOD);
    }


}
