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
package com.datastax.driver.core;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

import com.datastax.driver.core.exceptions.DiscoveryException;
import com.datastax.driver.core.policies.DiscoveryPolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;

/**
 * Default discovery strategy.
 * Relies on system.local and system.peers tables to discover peers in the cluster.
 */
public class DefaultDiscoveryPolicy implements DiscoveryPolicy {

    public static final class DefaultDiscoveryInfo implements DiscoveryInfo {

        private String clusterName;

        private String partitioner;

        private Map<Host, Collection<String>> tokenMap;

        private Set<Host> hosts;

        private Set<UUID> schemaVersions;

        public DefaultDiscoveryInfo(String clusterName, String partitioner, Set<Host> hosts, Map<Host, Collection<String>> tokenMap, Set<UUID> schemaVersions) {
            this.clusterName = clusterName;
            this.partitioner = partitioner;
            this.hosts = hosts;
            this.tokenMap = tokenMap;
            this.schemaVersions = schemaVersions;
        }

        @Override
        public String getClusterName() {
            return clusterName;
        }

        @Override
        public String getPartitioner() {
            return partitioner;
        }

        @Override
        public Set<Host> getHosts() {
            return hosts;
        }

        @Override
        public Map<Host, Collection<String>> getTokenMap() {
            return tokenMap;
        }

        @Override
        public Set<UUID> getSchemaVersions() {
            return schemaVersions;
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(DefaultDiscoveryPolicy.class);

    private static final String SELECT_PEERS = "SELECT * FROM system.peers";

    private static final String SELECT_LOCAL = "SELECT * FROM system.local WHERE key='local'";

    private static final InetAddress bindAllAddress;

    static {
        try {
            bindAllAddress = InetAddress.getByAddress(new byte[4]);
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    private Cluster cluster;

    @Override
    public void init(Cluster cluster) {
        checkNotNull(cluster);
        this.cluster = cluster;
    }

    @Override
    public DiscoveryInfo scan(Host.Factory factory, boolean initial) throws DiscoveryException, ExecutionException, InterruptedException {
        checkNotNull(cluster, "You must call init() prior to calling this method");
        logger.debug("[Discovery policy] start scan");
        Map<InetSocketAddress, Host> knownHosts = cluster.getAllHosts();
        DefaultResultSetFuture localFuture = localResultSetFuture();
        DefaultResultSetFuture peersFuture = allPeersResultSetFuture();
        Connection connection = getConnection();
        sendRequest(connection, localFuture);
        sendRequest(connection, peersFuture);
        String clusterName = null;
        String partitioner = null;
        ImmutableMap.Builder<Host, Collection<String>> tokenMap = ImmutableMap.builder();
        ImmutableSet.Builder<Host> hosts = ImmutableSet.builder();
        ImmutableSet.Builder<UUID> schemaVersions = ImmutableSet.builder();
        Row localRow = localFuture.get().one();
        if (localRow != null) {
            // Update cluster name, DC and rack for the one node we are connected to
            clusterName = localRow.getString("cluster_name");
            partitioner = localRow.getString("partitioner");
            InetSocketAddress connectedAddress = connection.address;
            Host connectedHost = knownHosts.get(connectedAddress);
            // In theory host can't be null. However there is no point in risking a NPE in case we
            // have a race between a node removal and this.
            if (connectedHost == null) {
                logger.debug("Host in local system table ({}) unknown to us (ok if said host just got removed)", connectedAddress);
            } else {
                updateHostInfo(connectedHost, localRow, initial);
                Set<String> tokens = localRow.getSet("tokens", String.class);
                if (partitioner != null && !tokens.isEmpty())
                    tokenMap.put(connectedHost, tokens);
                if (!localRow.isNull("schema_version"))
                    schemaVersions.add(localRow.getUUID("schema_version"));
                hosts.add(connectedHost);
            }
        }
        for (Row peerRow : peersFuture.get()) {
            InetSocketAddress peerAddress = addressToUseForPeer(connection, peerRow);
            if (peerAddress == null)
                continue;
            Host peer = knownHosts.get(peerAddress);
            if (peer == null) {
                // We don't know that node, create the Host object;
                // the control connection will then add it to the cluster metadata.
                peer = factory.newHost(peerAddress);
            }
            updateHostInfo(peer, peerRow, initial);
            if (partitioner != null) {
                Set<String> tokens = peerRow.getSet("tokens", String.class);
                if (!tokens.isEmpty())
                    tokenMap.put(peer, tokens);
            }
            if (peer.isUp() && !peerRow.isNull("schema_version")) {
                schemaVersions.add(peerRow.getUUID("schema_version"));
            }
            hosts.add(peer);
        }
        return new DefaultDiscoveryInfo(clusterName, partitioner, hosts.build(), tokenMap.build(), schemaVersions.build());
    }

    @Override
    public boolean refreshNodeInfo(Host host) throws DiscoveryException, ExecutionException, InterruptedException {
        checkNotNull(cluster, "You must call init() prior to calling this method");
        checkNotNull(host);
        Connection connection = getConnection();
        Row row = fetchRowForHost(connection, host);
        if (row == null) {
            if (connection.isDefunct()) {
                logger.debug("Control connection is down, could not refresh node info");
                // Keep going with what we currently know about the node, otherwise we will ignore all nodes
                // until the control connection is back up (which leads to a catch-22 if there is only one)
                return true;
            } else {
                logger.warn("No row found for host {} in {}'s peers system table. {} will be ignored.", host.getAddress(), connection.address, host.getAddress());
                return false;
            }
        } else {
            updateHostInfo(host, row, false);
            boolean connectedHost = connection.address.equals(host.getSocketAddress());
            if (!connectedHost && host.getAddress() == null) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void close() {
        this.cluster = null;
    }

    /**
     * @return the connection to use
     * @throws DiscoveryException
     */
    private Connection getConnection() {
        // reuse control connection if possible
        ControlConnection controlConnection = cluster.manager.controlConnection;
        if (controlConnection != null) {
            Connection connection = controlConnection.discoveryConnectionRef.get();
            if (connection != null)
                return connection;
        }
        throw new DiscoveryException("No connection available");
    }

    private ProtocolVersion getProtocolVersion() {
        return cluster.getConfiguration()
            .getProtocolOptions()
            .getProtocolVersionEnum();
    }

    private Row fetchRowForHost(Connection connection, Host host) throws DiscoveryException, ExecutionException, InterruptedException {
        boolean isLocal = connection.address.equals(host.getSocketAddress());
        if (isLocal) {
            return fetchRowFromSystemLocal(connection);
        } else {
            return fetchRowFromSystemPeers(connection, host);
        }
    }

    private Row fetchRowFromSystemLocal(Connection connection) throws DiscoveryException, ExecutionException, InterruptedException {
        DefaultResultSetFuture future = localResultSetFuture();
        sendRequest(connection, future);
        ResultSet rows = future.get();
        return rows.one();
    }

    private Row fetchRowFromSystemPeers(Connection connection, Host peer) throws DiscoveryException, ExecutionException, InterruptedException {
        if (peer.listenAddress != null) {
            DefaultResultSetFuture future = singlePeerResultSetFuture(peer);
            sendRequest(connection, future);
            ResultSet rows = future.get();
            return rows.one();
        } else {
            // We have to fetch the whole peers table and find the host we're looking for
            DefaultResultSetFuture future = allPeersResultSetFuture();
            sendRequest(connection, future);
            ResultSet rows = future.get();
            for (Row row : rows) {
                InetSocketAddress addr = addressToUseForPeer(connection, row);
                if (addr != null && addr.equals(peer.getSocketAddress()))
                    return row;
            }
            return null;
        }
    }

    private DefaultResultSetFuture localResultSetFuture() {
        return new DefaultResultSetFuture(null, getProtocolVersion(), new Requests.Query(SELECT_LOCAL));
    }

    private DefaultResultSetFuture allPeersResultSetFuture() {
        return new DefaultResultSetFuture(null, getProtocolVersion(), new Requests.Query(SELECT_PEERS));
    }

    private DefaultResultSetFuture singlePeerResultSetFuture(Host peer) {
        String query = SELECT_PEERS + " WHERE peer='" + peer.listenAddress.getHostAddress() + '\'';
        return new DefaultResultSetFuture(null, getProtocolVersion(), new Requests.Query(query));
    }

    private void sendRequest(Connection connection, DefaultResultSetFuture future) throws DiscoveryException {
        try {
            connection.write(future);
        } catch (ConnectionException e) {
            throw new DiscoveryException(e);
        } catch (BusyConnectionException e) {
            throw new DiscoveryException(e);
        }
    }

    private void updateHostInfo(Host host, Row row, boolean initial) {
        // row can come either from the 'local' table or the 'peers' one
        if (!row.isNull("data_center") || !row.isNull("rack"))
            updateHostLocationInfo(host, row.getString("data_center"), row.getString("rack"), initial);
        host.setVersion(row.getString("release_version"));
        // We don't know if it's a 'local' or a 'peers' row, and only 'peers' rows have the 'peer' field.
        InetAddress listenAddress = row.getColumnDefinitions().contains("peer")
            ? row.getInet("peer")
            : null;
        host.setListenAdress(listenAddress);
    }

    private void updateHostLocationInfo(Host host, String datacenter, String rack, boolean initial) {
        if (!Objects.equal(host.getDatacenter(), datacenter) || !Objects.equal(host.getRack(), rack)) {
            // If the dc/rack information changes for an existing node, we need to update the load balancing policy.
            // For that, we remove and re-add the node against the policy. Not the most elegant, and assumes
            // that the policy will update correctly, but in practice this should work.
            LoadBalancingPolicy loadBalancingPolicy = cluster.getConfiguration().getPolicies().getLoadBalancingPolicy();
            if (!initial) {
                loadBalancingPolicy.onDown(host);
            }
            host.setLocationInfo(datacenter, rack);
            if (!initial)
                loadBalancingPolicy.onAdd(host);
        }
    }

    private InetSocketAddress addressToUseForPeer(Connection connection, Row peerRow) {
        InetAddress listenAddress = peerRow.getInet("peer");
        InetAddress rpcAddress = peerRow.getInet("rpc_address");
        if (Objects.equal(listenAddress, connection.address.getAddress()) || Objects.equal(rpcAddress, connection.address.getAddress())) {
            // Some DSE versions were inserting a line for the local node in peers (with mostly null values). This has been fixed, but if we
            // detect that's the case, ignore it as it's not really a big deal.
            logger.debug("System.peers on node {} has a line for itself. This is not normal but is a known problem of some DSE versions. Ignoring the entry.", connection.address);
            return null;
        } else if (rpcAddress == null) {
            // Ignore hosts with a null rpc_address, as this is most likely a phantom row in system.peers (JAVA-428).
            // Don't test this for the control host since we're already connected to it anyway, and we read the info from system.local
            // which doesn't have an rpc_address column (JAVA-546).
            logger.warn("No rpc_address found for host {} in {}'s peers system table. {} will be ignored.", listenAddress, connection.address, listenAddress);
            return null;
        } else if (rpcAddress.equals(bindAllAddress)) {
            logger.warn("Found host with 0.0.0.0 as rpc_address, using listen_address ({}) to contact it instead. If this is incorrect you should avoid the use of 0.0.0.0 server side.", listenAddress);
            rpcAddress = listenAddress;
        }
        return translateAddress(rpcAddress);
    }

    private InetSocketAddress translateAddress(InetAddress address) {
        Configuration configuration = cluster.getConfiguration();
        int port = configuration.getProtocolOptions().getPort();
        InetSocketAddress original = new InetSocketAddress(address, port);
        InetSocketAddress translated = configuration.getPolicies().getAddressTranslater().translate(original);
        return translated == null ? original : translated;
    }

}
