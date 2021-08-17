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

package com.bytedance.bytehouse.bhit.scenario;

import com.bytedance.bytehouse.bhit.AbstractBHITEnvironment;
import com.bytedance.bytehouse.util.GenerateSqlInsertStatementUtil;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertTrue;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ByteHouseCustomerScenarioITest extends AbstractBHITEnvironment {

    @Test
    @Order(1)
    public void createDatabaseTest() throws SQLException, IOException {
        try (Connection connection = getEnvConnection()) {
            Statement stmt = connection.createStatement();
            stmt.execute(loadSqlStatement("create-database"));
        }
    }

    @Test
    @Order(2)
    public void createTableTest() throws SQLException, IOException {
        try (Connection connection = getEnvConnection()) {
            Statement stmt = connection.createStatement();

            stmt.execute(loadSqlStatement("create-table"));
        }
    }

    @Test
    @Order(3)
    public void insertIntoSingleColumn() throws SQLException, IOException {
        try (Connection connection = getEnvConnection()) {
            Statement stmt = connection.createStatement();
            stmt.executeUpdate(loadSqlStatement("insert-table-single-column"));
        }
    }

    @Test
    @Order(4)
    public void insertIntoSingleColumnMultipleTimes() throws SQLException, IOException {
        try (Connection connection = getEnvConnection()) {
            Statement stmt = connection.createStatement();

            for (int iter = 0; iter < 10; iter++) {
                stmt.executeUpdate(loadSqlStatement("insert-table-single-column"));
            }
        }
    }

    @Test
    @Order(5)
    public void insertIntoAllColumns() throws IOException, SQLException {
        try (Connection connection = getEnvConnection()) {
            Statement stmt = connection.createStatement();

            GenerateSqlInsertStatementUtil.generateInsertQuery("customer_database.customer_table");
            stmt.executeUpdate(loadSqlStatement("insert-table"));
        }
    }

    @Test
    @Order(6)
    public void insertIntoAllColumnsMultipleTimes() throws IOException, SQLException {
        try (Connection connection = getEnvConnection()) {
            Statement stmt = connection.createStatement();

            GenerateSqlInsertStatementUtil.generateInsertQuery("customer_database.customer_table");
            for (int iter = 0; iter < 10; iter++) {
                stmt.executeUpdate(loadSqlStatement("insert-table"));
            }
        }
    }

    @Test
    @Order(7)
    public void insertBatchTest() throws SQLException, IOException {
        try (Connection connection = getEnvConnection()) {
            PreparedStatement pstmt = connection.prepareStatement(loadSqlStatement("insert-batch"));
            int insertBatchSize = 1000;

            for (int i = 0; i < insertBatchSize; i++) {
                pstmt.setString(1, "Hello");
                pstmt.setString(2, "Byte");
                pstmt.setString(3, "House");
                pstmt.addBatch();
            }
            pstmt.executeBatch();
        }
    }

    @Test
    @Order(7)
    public void selectTableTest() throws SQLException, IOException {
        try (Connection connection = getEnvConnection()) {
            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(loadSqlStatement("select-table"));

            ResultSetMetaData rsmd = rs.getMetaData();
            int columnsNumber = rsmd.getColumnCount();
            while (rs.next()) {
                for (int i = 1; i <= columnsNumber; i++) {
                    String columnValue = rs.getString(i);
                }
            }
            assertTrue(columnsNumber > 0);
        }
    }

    @Test
    @Order(8)
    public void dropTableTest() throws SQLException, IOException {
        try (Connection connection = getEnvConnection()) {
            Statement stmt = connection.createStatement();

            stmt.execute(loadSqlStatement("drop-table"));
        }
    }

    @Test
    @Order(9)
    public void dropDatabaseTest() throws SQLException, IOException {
        try (Connection connection = getEnvConnection()) {
            Statement stmt = connection.createStatement();

            stmt.execute(loadSqlStatement("drop-database"));
        }
    }
}
