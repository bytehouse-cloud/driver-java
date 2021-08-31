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

    private static final String SERVER = "server";

    private Properties TestConfigs;
    private Properties EnvConfigs;
    private DataSource dataSource;
    private String lastUsedDatabaseName;
    private String lastUsedTableName;

    protected String getHost() {
        return TestConfigs.getProperty(HOST);
    }

    protected String getPort() {
        return TestConfigs.getProperty(PORT);
    }

    protected String getUrl() {
        return String.format("jdbc:bytehouse://%s:%s", getHost(), getPort());
    }

    protected String getCnchUrl() {
        return "jdbc:cnch:///dataexpress?secure=false";
    }

    protected String getUsername() {
        return TestConfigs.getProperty(USER);
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
        return EnvConfigs.getProperty(SERVER);
    }

    protected DataSource getDataSource() {
        return dataSource;
    }

    protected void setDataSource(DataSource ds) {
        this.dataSource = ds;
    }

    protected Connection getConnection(Object... params) throws SQLException {
        loadTestConfigs(params);

        if (getServerName().equals("gateway")) {
            final DataSource dataSource = new ByteHouseDataSource(getUrl(), TestConfigs);
            setDataSource(dataSource);
        }
        else if (getServerName().equals("cnch")) {
            final DataSource dataSource = new CnchRoutingDataSource(getCnchUrl(), TestConfigs);
            setDataSource(dataSource);
        }
        else {
            throw new SQLException("Server is not supported");
        }

        return getDataSource().getConnection();
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
        if (getServerName().equals("cnch")) {
            String uuid = getUuid(statement.getConnection(), lastUsedDatabaseName, lastUsedTableName);
            CnchRoutingDataSource dataSource = (CnchRoutingDataSource) getDataSource();
            statement = dataSource.getConnection(uuid).createStatement();
        }
        return statement;
    }

    protected ByteHouseDataSource getDataSource(String url, Object... params) throws SQLException {
        loadTestConfigs(params);
        final DataSource dataSource = new ByteHouseDataSource(url, TestConfigs);
        return (ByteHouseDataSource) dataSource;
    }

    private void loadTestConfigs(Object... params) {
        EnvConfigs = new Properties();
        TestConfigs = new Properties();
        try (InputStream input = Files.newInputStream(Paths
                .get("src/test/resources/env.properties"))) {
            EnvConfigs.load(input);
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }

        if (getServerName().equals("gateway")) {
            String envName = EnvConfigs.getProperty("env");

            try (InputStream input = Files.newInputStream(Paths
                    .get("src/test/resources/" + envName + "-config.properties"))) {
                TestConfigs.load(input);
            } catch (IOException ex) {
                throw new UncheckedIOException(ex);
            }
        }

        for (int i = 0; i + 1 < params.length; i = i + 2) {
            TestConfigs.setProperty(params[i].toString(), params[i+1].toString());
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
