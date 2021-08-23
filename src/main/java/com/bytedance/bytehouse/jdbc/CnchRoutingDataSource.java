/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.bytedance.bytehouse.jdbc;

import com.bytedance.bytehouse.jdbc.wrapper.BHDataSource;
import com.bytedance.bytehouse.log.Logger;
import com.bytedance.bytehouse.log.LoggerFactory;
import com.bytedance.bytehouse.routing.ConsulHelper;
import com.bytedance.bytehouse.routing.JumpConsistentHash;
import com.bytedance.bytehouse.settings.ByteHouseConfig;
import com.bytedance.bytehouse.settings.SettingKey;
import com.bytedance.commons.consul.Discovery;
import com.bytedance.commons.consul.ServiceNode;
import com.google.common.annotations.VisibleForTesting;
import java.io.PrintWriter;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * This datasource is specific to cnch with consul.
 * This datasource will automagically resolve cnch address for you.
 * the host and port passed in will be ignored. but the options will be accepted.
 * <br><br>
 * It enables the consul lookup and resolution of cnch's routing logic
 */
public class CnchRoutingDataSource implements BHDataSource {

    private static final Logger LOG = LoggerFactory.getLogger(CnchRoutingDataSource.class);

    private final List<ConnCreator> list = new ArrayList<>();

    private final ByteHouseConfig cfg;

    private final ConsulHelper consulHelper = new ConsulHelper(new Discovery());

    private final ReentrantReadWriteLock topologyLock = new ReentrantReadWriteLock();

    /**
     * create Datasource for bytehouse JDBC connections
     *
     * for example: <br>
     * <pre>
     * jdbc:cnch://localhost:9000/database?&option1=value1&option2=value2
     * </pre>
     *
     * @param url address for connection to the database, must have the next format
     * @throws IllegalArgumentException if param have not correct format,
     *                                  or error happens when checking host availability
     */
    public CnchRoutingDataSource(final String url) {
        this(url, new Properties());
    }

    /**
     * create Datasource for bytehouse JDBC connections
     *
     * @param url        address for connection to the database
     * @param properties database properties
     */
    public CnchRoutingDataSource(final String url, final Properties properties) {
        this(url, ByteHouseJdbcUrlParser.parseProperties(properties));
    }

    /**
     * create Datasource for bytehouse JDBC connections
     *
     * @param url      address for connection to the database
     * @param settings bytehouse settings
     */
    private CnchRoutingDataSource(
            final String url,
            final Map<SettingKey, Serializable> settings
    ) {
        this.cfg = ByteHouseConfig.Builder.builder()
                .withJdbcUrl(url)
                .withSettings(settings)
                .host("toBeReplacedLater")
                .port(0)
                .build();
    }

    /**
     * Used for production setting to discover IPs from consul.
     *
     * @throws Exception    might throw exception from consul API. Due to the lack of documentation
     *                      on their side, we can't know for sure.
     * @throws SQLException when consul returns empty list.
     */
    @VisibleForTesting
    List<ConnCreator> initializeWithConsul(
            final String dbHost,
            final String username,
            final String password

    ) throws SQLException {
        final List<ServiceNode> dbServiceNodes = consulHelper.discoverServiceNodes(dbHost);
        if (dbServiceNodes.isEmpty()) {
            throw new SQLException("No CNCH servers are discovered through consul");
        }
        final List<ConnCreator> generators = new ArrayList<>();
        for (final ServiceNode node : dbServiceNodes) {
            final ByteHouseConfig byteHouseConfig = this.cfg
                    .withHostPort(
                            consulHelper.getIpv4FromNode(node),
                            consulHelper.getTcpPortFromNode(node)
                    )
                    .withCredentials(username, password);
            generators.add(new ConnCreator(
                    consulHelper.getK8sServiceAddr(node),
                    byteHouseConfig
            ));
        }

        return generators;
    }

    private boolean isTopologyLoaded() {
        topologyLock.readLock().lock();
        try {
            return !list.isEmpty();
        } finally {
            topologyLock.readLock().unlock();
        }
    }

    public ByteHouseConnection getConnection(
            final String tableUuid
    ) throws SQLException {
        if (!isTopologyLoaded()) {
            this.loadTopology();
        }

        topologyLock.readLock().lock();
        try {
            if (tableUuid == null) {
                return list.get(ThreadLocalRandom.current().nextInt(list.size())).create();
            } else {
                final int index = JumpConsistentHash
                        .consistentHashForString(tableUuid, list.size());
                return list.get(index).create();
            }
        } finally {
            topologyLock.readLock().unlock();
        }
    }

    @Override
    public Connection getConnection() throws SQLException {
        return getConnection(null);
    }

    @Override
    public Connection getConnection(
            final String username,
            final String password
    ) throws SQLException {
        return getConnection(null);
    }

    public void loadTopology() throws SQLException {
        LOG.info("Lazy initializing topology");
        topologyLock.writeLock().lock();
        try {
            if (!list.isEmpty()) {
                return; // topology is already loaded.
            }
            List<ConnCreator> generators;
            try {
                generators = initializeWithConsul(
                        "consul:data.cnch.server:default",
                        cfg.user(),
                        cfg.password()
                );
            } catch (Exception ex) { //NOPMD catch everything
                throw new SQLException(ex);
            }
            if (generators.isEmpty()) {
                throw new SQLException("failed to get any IPs");
            }

            list.addAll(generators);
            Collections.sort(list);
        } finally {
            topologyLock.writeLock().unlock();
        }
    }

    public void unloadTopology() {
        topologyLock.writeLock().lock();
        try {
            list.clear();
        } finally {
            topologyLock.writeLock().unlock();
        }
    }

    @Override
    public boolean ping(final int timeoutSecond) {
        try (ByteHouseConnection connection = (ByteHouseConnection) getConnection()) {
            return connection.ping(Duration.ofSeconds(timeoutSecond));
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return null;
    }

    @Override
    public void setLogWriter(final PrintWriter out) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setLoginTimeout(final int seconds) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }

    /**
     * underlying holders of connection objects.
     */
    @VisibleForTesting
    static class ConnCreator implements Comparable<ConnCreator> {

        private final String k8sServiceAddr;

        private final ByteHouseConfig config;

        /**
         * Constructor.
         */
        //false alarm
        @SuppressWarnings({"PMD.ConstructorCallsOverridableMethod", "PMD.CloseResource"})
        public ConnCreator(
                final String k8sServiceAddr,
                final ByteHouseConfig config
        ) {
            this.k8sServiceAddr = k8sServiceAddr;
            this.config = config;
        }

        /**
         * creates a connection. All the info required to generate a connection
         * must be included in the class. Hence this method can self-generate connections.
         */
        ByteHouseConnection create() throws SQLException {
            LOG.info("connecting to {}:{}", config.host(), config.port());
            return ByteHouseConnection.createByteHouseConnection(config);
        }

        @Override
        public int compareTo(final ConnCreator theOther) {
            return this.k8sServiceAddr.compareTo(theOther.k8sServiceAddr);
        }
    }
}
