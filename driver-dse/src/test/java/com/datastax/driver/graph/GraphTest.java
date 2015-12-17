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
import com.google.common.collect.Iterators;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Iterator;

public class GraphTest {

    private GraphSession session;

    @BeforeMethod(groups = "unit")
    public void setUpCluster() {
        Cluster cluster = Cluster.builder()
                .addContactPoint("127.0.0.1")
                .withQueryOptions(new GraphQueryOptions().setGraphKeyspace("comics"))
                .build();
        GraphCluster graphCluster = new GraphCluster(cluster);
        session = graphCluster.connect();
    }

    @Test(groups = "unit")
    public void test1() throws Exception {
        GraphResultSet rs = session.executeGraph(new SimpleGraphStatement("g.V()"));
        for (GraphResult result : rs) {
            printVertex(result);
        }
    }

    @Test(groups = "unit")
    public void test2() throws Exception {
        GraphResultSet rs = session.executeGraph(new SimpleGraphStatement("g.E()"));
        for (GraphResult result : rs) {
            printEdge(result);
        }
    }

    @Test(groups = "unit")
    public void test3() throws Exception {
        GraphResultSet rs = session.executeGraph(new SimpleGraphStatement("g.V().label()"));
        for (GraphResult result : rs) {
            String label = result.asString();
            System.out.println(label);
        }
    }

    @Test(groups = "unit")
    public void test4() throws Exception {
        SimpleGraphStatement stmt = new SimpleGraphStatement("g.V().hasLabel('character').has('name', name)");
        stmt.set("name", "BLACK PANTHER/T'CHAL");
        GraphResultSet rs = session.executeGraph(stmt);
        for (GraphResult result : rs) {
            printVertex(result);
        }
    }

    private void printVertex(GraphResult result) {
        Vertex vertex = result.asVertex();
        System.out.println(vertex);
        Iterator<Edge> edges = vertex.edges(Direction.BOTH);
        System.out.println(Iterators.toString(edges));
    }

    private void printEdge(GraphResult result) {
        Edge edge = result.asEdge();
        System.out.println(edge);
        Vertex in = edge.inVertex();
        System.out.println(in);
        Iterator<Edge> edges = in.edges(Direction.BOTH);
        System.out.println(Iterators.toString(edges));
    }

}
