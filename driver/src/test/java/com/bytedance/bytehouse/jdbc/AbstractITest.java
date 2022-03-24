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
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.Random;
import javax.sql.DataSource;

public class AbstractITest implements Serializable {
    private static final String REGION = "REGION";
    private static final String USER = "USER";

    private Properties driverSettings;

    private DataSource dataSource;
    private String lastUsedDatabaseName;
    private String lastUsedTableName;

    protected String getRegion() {
        return driverSettings.getProperty(REGION);
    }

    protected String getUrl() {
        return String.format("jdbc:bytehouse:///?region=%s", getRegion());
    }

    protected String getCreateTableSuffix() {
        return "ENGINE=CnchMergeTree() order by tuple()";
    }

    protected String getUsername() {
        return driverSettings.getProperty(USER);
    }

    protected String getDatabaseName() {
        lastUsedDatabaseName = "jdbc_test_db_" + generateRandomString();
        return lastUsedDatabaseName;
    }

    protected String getTableName() {
        lastUsedTableName = "jdbc_test_db_" + generateRandomString();
        return lastUsedTableName;
    }

    protected DataSource getDataSource() {
        return dataSource;
    }

    protected Connection getConnection(Object... params) throws SQLException {
        loadTestConfigs(params);
        dataSource = new ByteHouseDataSource(getUrl(), driverSettings);
        return dataSource.getConnection();
    }

    protected Statement getStatement(Statement statement) throws SQLException {
        return statement;
    }

    protected ByteHouseDataSource getDataSource(String url, Object... params) throws SQLException {
        loadTestConfigs(params);
        final DataSource dataSource = new ByteHouseDataSource(url, driverSettings);
        return (ByteHouseDataSource) dataSource;
    }

    private void loadTestConfigs(Object... params) {
        driverSettings = new Properties();

        try (InputStream input = Files.newInputStream(Paths
                .get("src/test/resources/config.properties"))) {
            driverSettings.load(input);
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }

        for (int i = 0; i + 1 < params.length; i = i + 2) {
            driverSettings.setProperty(params[i].toString(), params[i+1].toString());
        }
    }

    protected String generateRandomString() {
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
