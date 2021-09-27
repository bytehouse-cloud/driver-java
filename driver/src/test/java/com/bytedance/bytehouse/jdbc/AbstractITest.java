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

import com.github.housepower.jdbc.BalancedClickhouseDataSource;
import org.testcontainers.containers.ClickHouseContainer;
import ru.yandex.clickhouse.ClickHouseDataSource;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.Random;
import javax.sql.DataSource;

public class AbstractITest implements Serializable {
    private static final String HOST = "HOST";
    private static final String PORT = "PORT";
    private static final String USER = "USER";

    private Properties TestConfigs;
    protected static final String DRIVER = "driver";
    protected static final String BYTEHOUSE = "bytehouse";
    protected static final String CLICKHOUSE_NATIVE = "clickhouse-native";
    protected static final String CLICKHOUSE_HTTP = "clickhouse-http";
    private static final String SERVER = "server";
    private static final String GATEWAY = "gateway";
    private static final String CNCH = "cnch";
    private static final String ENV = "env";

    private Properties DriverSettings;

    private DataSource dataSource;
    private String lastUsedDatabaseName;
    private String lastUsedTableName;

    private static final String CLICKHOUSE_IMAGE = "yandex/clickhouse-server:21.3";
    private static final ClickHouseContainer container;

    static {
        container = new ClickHouseContainer(CLICKHOUSE_IMAGE);
        container.start();
    }

    protected String getHost() {
        return DriverSettings.getProperty(HOST);
    }

    protected String getPort() {
        return DriverSettings.getProperty(PORT);
    }

    protected String getUrl() {
        switch (getDriver()) {
            case BYTEHOUSE:
                switch (getServerName()) {
                    case GATEWAY:
                        return String.format("jdbc:bytehouse://%s:%s", getHost(), getPort());
                    case CNCH:
                        return "jdbc:cnch:///dataexpress?secure=false";
                }
            case CLICKHOUSE_NATIVE:
                return "jdbc:clickhouse://" + container.getHost() + ":" + container.getMappedPort(ClickHouseContainer.NATIVE_PORT);
            case CLICKHOUSE_HTTP:
                return "jdbc:clickhouse://" + container.getHost() + ":" + container.getMappedPort(ClickHouseContainer.HTTP_PORT);
        }
        return "";
    }

    protected String getCreateTableSuffix() {
        if (getDriver().equals(BYTEHOUSE)) {
            return "ENGINE=CnchMergeTree() order by tuple()";
        }
        else {
            return "ENGINE=MergeTree() order by tuple()";
        }
    }

    protected String getUsername() {
        return DriverSettings.getProperty(USER);
    }

    protected String getDatabaseName() {
        lastUsedDatabaseName = "jdbc_test_db_" + generateRandomString();
        return lastUsedDatabaseName;
    }

    protected String getTableName() {
        lastUsedTableName = "jdbc_test_db_" + generateRandomString();
        return lastUsedTableName;
    }

    protected String getServerName() {
        return TestConfigs.getProperty(SERVER);
    }

    protected String getDriver() {
        return TestConfigs.getProperty(DRIVER);
    }

    protected DataSource getDataSource() {
        return dataSource;
    }

    protected Connection getConnection(Object... params) throws SQLException {
        loadTestConfigs(params);

        switch (getDriver()) {
            case BYTEHOUSE:
                switch (getServerName()) {
                    case GATEWAY:
                        dataSource = new ByteHouseDataSource(getUrl(), DriverSettings);
                        break;
                    case CNCH:
                        dataSource = new CnchRoutingDataSource(getUrl(), DriverSettings);
                        break;
                }
                break;
            case CLICKHOUSE_NATIVE:
                dataSource = new BalancedClickhouseDataSource(getUrl(), DriverSettings);
                break;
            case CLICKHOUSE_HTTP:
                dataSource = new ClickHouseDataSource(getUrl(), DriverSettings);
                break;
        }

        return dataSource.getConnection();
    }

    protected String getUuid(Connection connection, String database, String table) throws SQLException {
        final ResultSet resultSet = connection
                .createStatement()
                .executeQuery(String.format("select uuid from system.cnch_tables where database = '%s' and name = '%s'", database, table));

        if (resultSet.next()) {
            return resultSet.getString("uuid");
        } else {
            throw new SQLException("Failed to get uuid from resultset");
        }
    }

    protected Statement getStatement(Statement statement) throws SQLException {
        if (getServerName().equals(CNCH)) {
            String uuid = getUuid(statement.getConnection(), lastUsedDatabaseName, lastUsedTableName);
            CnchRoutingDataSource dataSource = (CnchRoutingDataSource) getDataSource();
            statement = dataSource.getConnection(uuid).createStatement();
        }
        return statement;
    }

    protected ByteHouseDataSource getDataSource(String url, Object... params) throws SQLException {
        loadTestConfigs(params);
        final DataSource dataSource = new ByteHouseDataSource(url, DriverSettings);
        return (ByteHouseDataSource) dataSource;
    }

    private void loadTestConfigs(Object... params) {
        TestConfigs = new Properties();
        DriverSettings = new Properties();
        try (InputStream input = Files.newInputStream(Paths
                .get("src/test/resources/config.properties"))) {
            TestConfigs.load(input);
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }

        if (getServerName().equals(GATEWAY)) {
            String envName = TestConfigs.getProperty(ENV);

            try (InputStream input = Files.newInputStream(Paths
                    .get("src/test/resources/" + envName + "-config.properties"))) {
                DriverSettings.load(input);
            } catch (IOException ex) {
                throw new UncheckedIOException(ex);
            }
        }

        for (int i = 0; i + 1 < params.length; i = i + 2) {
            DriverSettings.setProperty(params[i].toString(), params[i+1].toString());
        }
    }

    private String generateRandomString() {
        int leftLimit = 97; // letter 'a'
        int rightLimit = 122; // letter 'z'
        int targetStringLength = 10;
        Random random = new Random();

        String generatedString = random.ints(leftLimit, rightLimit + 1)
                .limit(targetStringLength)
                .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                .toString();

        return generatedString;
    }

    protected void withNewConnection(WithConnection withConnection, Object... args) throws Exception {
        try (Connection connection = getConnection(args)) {
            withConnection.apply(connection);
        }
    }

    protected void withNewConnection(DataSource ds, WithConnection withConnection) throws Exception {
        try (Connection connection = ds.getConnection()) {
            withConnection.apply(connection);
        }
    }

    protected void withStatement(Connection connection, WithStatement withStatement) throws Exception {
        try (Statement stmt = connection.createStatement()) {
            withStatement.apply(stmt);
        }
    }

    protected void withStatement(WithStatement withStatement, Object... args) throws Exception {
        withNewConnection(connection -> withStatement(connection, withStatement), args);
    }

    protected void withPreparedStatement(Connection connection,
                                         String sql,
                                         WithPreparedStatement withPreparedStatement) throws Exception {
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            withPreparedStatement.apply(pstmt);
        }
    }

    protected void withPreparedStatement(String sql,
                                         WithPreparedStatement withPreparedStatement,
                                         Object... args) throws Exception {
        withNewConnection(connection -> withPreparedStatement(connection, sql, withPreparedStatement), args);
    }

    @FunctionalInterface
    public interface WithConnection {
        void apply(Connection connection) throws Exception;
    }

    @FunctionalInterface
    public interface WithStatement {
        void apply(Statement stmt) throws Exception;
    }

    @FunctionalInterface
    public interface WithPreparedStatement {
        void apply(PreparedStatement pstmt) throws Exception;
    }
}
