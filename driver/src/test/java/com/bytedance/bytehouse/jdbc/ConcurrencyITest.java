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

import org.junit.jupiter.api.Test;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.*;

public class ConcurrencyITest extends AbstractITest {

    @Test
    public void withAllNewConnections() throws Exception {
        int threadCount = 10;
        List<Connection> connectionList = new ArrayList<>();
        List<Future<Integer>> results;

        try {
            ExecutorService executor = Executors.newFixedThreadPool(threadCount);
            List<Callable<Integer>> futureList = new ArrayList<>();

            for (int i=0; i<threadCount; i++) {
                String databaseName = getDatabaseName();
                String tableName = databaseName + "." + getTableName();
                Connection connection = getConnection();
                Statement statement = connection.createStatement();
                futureList.add(new ConcurrentTest(connection, statement, databaseName, tableName));
                connectionList.add(connection);
            }
            results = executor.invokeAll(futureList);
            executor.shutdown();
        }
        finally {
            for (Connection connection: connectionList) {
                if (connection != null) {
                    connection.close();
                }
            }
        }

        for (Future<Integer> future: results) {
            assertEquals((int) future.get(), 1);
        }
    }

    public class ConcurrentTest implements Callable<Integer> {

        public Connection connection;
        public Statement statement;
        public String databaseName;
        public String tableName;

        public ConcurrentTest(Connection connection, Statement statement, String databaseName, String tableName) {
            this.connection = connection;
            this.statement = statement;
            this.databaseName = databaseName;
            this.tableName = tableName;
        }

        @Override
        public Integer call() throws Exception {
            try {
                statement.execute(String.format("DROP DATABASE IF EXISTS %s", databaseName));
                statement.execute(String.format("CREATE DATABASE IF NOT EXISTS %s", databaseName));
                statement.execute(String.format("CREATE TABLE IF NOT EXISTS %s (num INT) %s", tableName, getCreateTableSuffix()));

                PreparedStatement preparedStatement = connection.prepareStatement(
                        String.format("INSERT INTO %s VALUES (?)", tableName, getCreateTableSuffix())
                );
                int startIdx = new Random().nextInt(100);
                int batchSize = startIdx;

                for (int iter=startIdx; iter<startIdx+batchSize; iter++) {
                    preparedStatement.setInt(1, iter);
                    preparedStatement.addBatch();
                }

                assertEquals(preparedStatement.executeBatch().length, batchSize);

                ResultSet rs = statement.executeQuery(String.format("SELECT * FROM %s", tableName));
                for (int iter=startIdx; iter<startIdx+batchSize; iter++) {
                    assertTrue(rs.next());
                    assertEquals(rs.getInt(1), iter);
                }
                assertFalse(rs.next());

                return 1;

            } catch (SQLException exception) {
                exception.printStackTrace();
            }
            finally {
                statement.execute(String.format("DROP DATABASE IF EXISTS %s", databaseName));
            }
            return 0;
        }
    }
}
