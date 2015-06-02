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
package com.datastax.driver.core.policies;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.exceptions.DiscoveryException;

/**
 * Defines a strategy to discover peers in the cluster and information about it,
 * such as its name, the partitioner in use, token maps and schema versions.
 */
public interface DiscoveryPolicy {

    /**
     * Gathers information discovered about the cluster
     * during a scan process.
     *
     * @see #scan(Host.Factory, boolean)
     */
    interface DiscoveryInfo {

        String getClusterName();

        String getPartitioner();

        Set<Host> getHosts();

        Map<Host, Collection<String>> getTokenMap();

        Set<UUID> getSchemaVersions();
    }

    /**
     * Called at cluster initialization.
     *
     * @param cluster the cluster whose peers are to be discovered.
     */
    void init(Cluster cluster);

    /**
     * Perform a full scan of the cluster and return all the nodes found
     * along with information about the cluster.
     *
     * @param factory the Host factory to use.
     * @param initial true it is the first scan, false otherwise
     * @return discovered nodes
     * @throws DiscoveryException
     * @throws ExecutionException
     * @throws InterruptedException
     */
    DiscoveryInfo scan(Host.Factory factory, boolean initial) throws DiscoveryException, ExecutionException, InterruptedException;

    /**
     * Refresh information for the given host.
     *
     * @param host the host to refresh.
     * @return true if succeeded, false otherwise.
     * @throws DiscoveryException
     * @throws ExecutionException
     * @throws InterruptedException
     */
    boolean refreshNodeInfo(Host host) throws DiscoveryException, ExecutionException, InterruptedException;

    /**
     * Called on cluster close.
     */
    void close();

}
