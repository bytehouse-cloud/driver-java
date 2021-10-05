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

@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
public class AbstractBenchmark {
    private static final String HOST = "HOST";
    private static final String PORT = "PORT";

    private static final String GATEWAY = "gateway";
    private static final String CNCH = "cnch";
    private static final String CLICKHOUSE = "clickhouse";
    private static final String LOCAL = "local";
    private static final String POD = "pod";
    private static final String ENV = "env";
    private static final String SERVER = "server";
    private static final String DRIVER = "driver";
    private static final String BYTEHOUSE = "bytehouse";

    private static final String CLICKHOUSE_IMAGE = "yandex/clickhouse-server:21.3";
    private static final ClickHouseContainer CONTAINER;

    protected String databaseName = "jdbc_benchmark";
    protected String tableName = "jdbc_benchmark";
    protected RandomGenerator randomGenerator = new RandomGenerator();

    private Properties envConfigs;
    private Properties testConfigs;

    private Connection connection;
    private DataSource dataSource;
    private Statement statement;
    private String uuid;

    static {
        CONTAINER = new ClickHouseContainer(CLICKHOUSE_IMAGE);
        CONTAINER.start();
    }

    protected void init(String databaseName, String tableName, String createTableSql) throws SQLException {
        loadTestConfigs();
        connection = getNewConnection();
        statement = connection.createStatement();

        String dropDatabaseSql = "DROP DATABASE IF EXISTS " + databaseName;
        String createDatabaseSql = "CREATE DATABASE " + databaseName;
        statement.execute(dropDatabaseSql);
        statement.execute(createDatabaseSql);

        if (!createTableSql.equals("")) {
            String createTableSqlWithEngine = createTableSql;
            if (getServerName().equals(CLICKHOUSE)) {
                createTableSqlWithEngine += "Engine = Log";
            }
            else {
                createTableSqlWithEngine += "ENGINE=CnchMergeTree() order by tuple()";
            }
            statement.execute(createTableSqlWithEngine);
        }

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
        if (getDriverName().equals(CLICKHOUSE)) {
            dataSource = new com.github.housepower.jdbc.BalancedClickhouseDataSource(getUrl(), testConfigs);
        }
        else if (getDriverName().equals(BYTEHOUSE)) {
            if (getServerName().equals(CNCH)) {
                dataSource = new CnchRoutingDataSource(getUrl(), testConfigs);
            }
            else if (getServerName().equals(CLICKHOUSE)) {
                dataSource = new ByteHouseDataSource(getUrl(), testConfigs);
                //TODO: Throwing Exception Error sql: select timezone()
            }
            else {
                dataSource = new ByteHouseDataSource(getUrl(), testConfigs);
            }
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
        return testConfigs.getProperty(HOST);
    }

    private String getPort() {
        return testConfigs.getProperty(PORT);
    }

    private String getUrl() {
        switch (envConfigs.getProperty(SERVER)) {
            case CNCH:
                return "jdbc:cnch:///dataexpress?secure=false";
            case CLICKHOUSE:
                return "jdbc:" + getDriverName() + "://" + CONTAINER.getHost() + ":" + CONTAINER.getMappedPort(ClickHouseContainer.NATIVE_PORT);
            default:
                return String.format("jdbc:bytehouse://%s:%s", getHost(), getPort());

        }
    }

    private String getServerName() {
        return envConfigs.getProperty(SERVER);
    }

    private String getDriverName() {
        return envConfigs.getProperty(DRIVER);
    }

    private void loadTestConfigs() {
        envConfigs = new Properties();
        String envConfigsResourcePath = AbstractBenchmark.class.getResource("/env.properties").getPath();

        try (InputStream input = Files.newInputStream(Paths.get(envConfigsResourcePath))) {
            envConfigs.load(input);
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }

        testConfigs = new Properties();
        String testConfigsResourcePath = AbstractBenchmark.class.getResource("/" + envConfigs.getProperty(ENV) + "-" + envConfigs.getProperty(SERVER) + "-config.properties").getPath();
        try (InputStream input = Files.newInputStream(Paths.get(testConfigsResourcePath))) {
            testConfigs.load(input);
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    protected String getColumnsExpression(Type type, int columnCount) {
        String baseExpression = type.toString();
        StringBuilder expressionBuilder = new StringBuilder();
        for (int i=0; i<columnCount; i++) {
            expressionBuilder.append("col" + i + " " + baseExpression);
            if (i != columnCount - 1) expressionBuilder.append(", ");
        }
        return expressionBuilder.toString();
    }

    protected String getExclaimExpression(int columnCount) {
        StringBuilder expressionBuilder = new StringBuilder();
        for (int i=0; i<columnCount; i++) {
            expressionBuilder.append("?");
            if (i != columnCount - 1) expressionBuilder.append(", ");
        }
        return expressionBuilder.toString();
    }
}
