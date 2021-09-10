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
package com.bytedance.bytehouse;

import com.bytedance.bytehouse.jdbc.ByteHouseDataSource;
import com.bytedance.bytehouse.jdbc.CnchRoutingDataSource;
import com.bytedance.bytehouse.misc.StrUtil;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.testcontainers.containers.ClickHouseContainer;
import org.testcontainers.junit.jupiter.Container;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
public class AbstractIBenchmark {
    private static final String HOST = "HOST";
    private static final String PORT = "PORT";

    private static final String GATEWAY = "gateway";
    private static final String CNCH = "cnch";
    private static final String CLICKHOUSE = "clickhouse";
    private static final String LOCAL = "local";
    private static final String POD = "pod";
    private static final String ENV = "env";
    private static final String SERVER = "server";

    private static final String CLICKHOUSE_IMAGE = "yandex/clickhouse-server:21.3";
    private static final String CLICKHOUSE_USER = "default";
    private static final String CLICKHOUSE_PASSWORD = "";
    private static final String CLICKHOUSE_DB = "";

    protected static String CK_HOST;
    protected static String CK_IP;
    protected static int CK_PORT;

    private Properties EnvConfigs;
    private Properties TestConfigs;

    private Connection connection;
    private DataSource dataSource;
    private Statement statement;
    private String uuid;

    protected void init(String databaseName, String tableName, String createTableSql) throws SQLException {
        loadTestConfigs();
        connection = getNewConnection();
        statement = connection.createStatement();

        String dropDatabaseSql = "DROP DATABASE IF EXISTS " + databaseName;
        String createDatabaseSql = "CREATE DATABASE " + databaseName;
        statement.execute(dropDatabaseSql);
        statement.execute(createDatabaseSql);

        if (getServerName().equals(CLICKHOUSE)) {
            createTableSql += "Engine = Log";
        }
        else {
            createTableSql += "ENGINE=CnchMergeTree() order by tuple()";
        }
        statement.execute(createTableSql);

        if (getServerName().equals(CNCH)) {
            uuid = getUuid(connection, databaseName, tableName);
        }
        statement = getStatementWithUUID();
    }

    protected void teardown(String databaseName) throws SQLException {
        String dropDatabaseSql = "DROP DATABASE IF EXISTS " + databaseName;
        statement.execute(dropDatabaseSql);
        connection.close();
    }

    protected Connection getConnection() {
        return connection;
    }

    protected Statement getStatement() {
        return statement;
    }

    protected PreparedStatement getPreparedStatement(String sql) throws SQLException {
        if (getServerName().equals(CNCH)) {
            return ((CnchRoutingDataSource) dataSource).getConnection(uuid).prepareStatement(sql);
        } else {
            return connection.prepareStatement(sql);
        }
    }

    private Connection getNewConnection() throws SQLException {
        if (getServerName().equals(CNCH)) {
            dataSource = new CnchRoutingDataSource(getUrl(), TestConfigs);
        }
        else if (getServerName().equals(CLICKHOUSE)) {
            dataSource = new ByteHouseDataSource(getUrl(), TestConfigs);
            //TODO: Throwing Exception Error sql: select timezone()
        }
        else {
            dataSource = new ByteHouseDataSource(getUrl(), TestConfigs);
        }
        return dataSource.getConnection();
    }

    private Statement getStatementWithUUID() throws SQLException {
        if (getServerName().equals(CNCH)) {
            return ((CnchRoutingDataSource) dataSource).getConnection(uuid).createStatement();
        } else {
            return statement;
        }
    }

    private String getUuid(Connection connection, String databaseName, String tableName) throws SQLException {
        final ResultSet resultSet = connection
                .createStatement()
                .executeQuery(String.format("select uuid from system.cnch_tables where database = '%s' and name = '%s'", databaseName, tableName));

        if (resultSet.next()) {
            return resultSet.getString("uuid");
        } else {
            throw new SQLException("Failed to get uuid from resultset");
        }
    }

    private String getHost() {
        return TestConfigs.getProperty(HOST);
    }

    private String getPort() {
        return TestConfigs.getProperty(PORT);
    }

    private String getUrl() {
        switch (EnvConfigs.getProperty(SERVER)) {
            case CNCH:
                return "jdbc:cnch:///dataexpress?secure=false";
            case CLICKHOUSE:
                extractContainerInfo();
                StringBuilder sb = new StringBuilder();
                int port = CK_PORT;
                sb.append("jdbc:bytehouse://").append(container.getHost()).append(":").append(port);
                if (StrUtil.isNotEmpty(CLICKHOUSE_DB)) {
                    sb.append("/").append(container.getDatabaseName());
                }
                sb.append("user=").append(container.getUsername());
                if (!StrUtil.isBlank(CLICKHOUSE_PASSWORD)) {
                    sb.append("&password=").append(container.getPassword());
                }
                return sb.toString();
            default:
                return String.format("jdbc:bytehouse://%s:%s", getHost(), getPort());

        }
    }

    @Container
    private static ClickHouseContainer container = (ClickHouseContainer) new ClickHouseContainer(CLICKHOUSE_IMAGE)
            .withEnv("CLICKHOUSE_USER", CLICKHOUSE_USER)
            .withEnv("CLICKHOUSE_PASSWORD", CLICKHOUSE_PASSWORD)
            .withEnv("CLICKHOUSE_DB", CLICKHOUSE_DB);

    public static void extractContainerInfo() {
        ClickHouseContainer container = (ClickHouseContainer) new ClickHouseContainer(CLICKHOUSE_IMAGE)
                .withEnv("CLICKHOUSE_USER", CLICKHOUSE_USER)
                .withEnv("CLICKHOUSE_PASSWORD", CLICKHOUSE_PASSWORD)
                .withEnv("CLICKHOUSE_DB", CLICKHOUSE_DB);
        container.start();
        CK_HOST = container.getHost();
        CK_IP = container.getContainerIpAddress();
        CK_PORT = container.getMappedPort(ClickHouseContainer.NATIVE_PORT);
    }

    private String getServerName() {
        return EnvConfigs.getProperty(SERVER);
    }

    private void loadTestConfigs() {
        EnvConfigs = new Properties();
        String envConfigsResourcePath = "benchmark/resources/env.properties";

        try (InputStream input = Files.newInputStream(Paths.get(envConfigsResourcePath))) {
            EnvConfigs.load(input);
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }

        TestConfigs = new Properties();
        String testConfigsResourcePath = "benchmark/resources/" + EnvConfigs.getProperty(ENV) + "-" + EnvConfigs.getProperty(SERVER) + "-config.properties";
        try (InputStream input = Files.newInputStream(Paths.get(testConfigsResourcePath))) {
            TestConfigs.load(input);
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }
}
