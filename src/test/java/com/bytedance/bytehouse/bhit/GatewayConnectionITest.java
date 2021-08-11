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

package com.bytedance.bytehouse.bhit;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.Test;

/**
 * Tests basic functionalities with ByteHouse.
 */
public class GatewayConnectionITest extends AbstractByteHouseITest {

    @Test
    public void runSimpleQuery_success() throws SQLException {
        try (Connection connection = getConnection()) {
            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT 1");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertFalse(rs.next());
        }
    }

    @Test
    public void runComplexQuery_success() throws SQLException {
        try (Connection connection = getConnection()) {
            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(
                    "SELECT (number % 3 + 1) as n, sum(number) FROM numbers(10000000) GROUP BY n"
            );
            for (int i = 1; i <= 3; i++) {
                assertTrue(rs.next());
                assertEquals(i, rs.getInt(1));
            }
        }
    }

    @Test
    public void showWarehouses_success() throws SQLException {
        try (Connection connection = getConnection()) {
            Statement stmt = connection.createStatement();
            assertDoesNotThrow(() -> stmt.execute("SHOW WAREHOUSES"));
        }
    }

    @Test
    public void createDatabase_success() throws SQLException {
        Connection connection = getConnection();
        Statement stmt = connection.createStatement();
        assertDoesNotThrow(() -> stmt.execute("CREATE DATABASE IF NOT EXISTS jdbc_test_db"));
        assertDoesNotThrow(() -> stmt.execute("DROP DATABASE jdbc_test_db"));
        connection.close();
    }

    @Test
    public void createTable_success() throws SQLException {
        try (Connection connection = getConnection()) {
            Statement stmt = connection.createStatement();
            assertDoesNotThrow(() -> stmt.execute("CREATE DATABASE IF NOT EXISTS jdbc_test_db"));
            assertDoesNotThrow(() -> {
                stmt.execute(
                        "CREATE TABLE IF NOT EXISTS jdbc_test_db.npc_cases\n" +
                                "(" +
                                "    year Int16," +
                                "    npc String," +
                                "    offence String," +
                                "    case_no Int32" +
                                ")" +
                                "    engine = CnchMergeTree()" +
                                "    partition by year" +
                                "    order by year"
                );
            });
            assertDoesNotThrow(() -> stmt.execute("DROP DATABASE jdbc_test_db"));
        }
    }

    @Test
    public void insert_success() throws SQLException {
        try (Connection connection = getConnection()) {
            Statement stmt = connection.createStatement();
            assertDoesNotThrow(() -> stmt.execute("CREATE DATABASE IF NOT EXISTS jdbc_test_db"));
            assertDoesNotThrow(() -> {
                stmt.execute(
                        "CREATE TABLE IF NOT EXISTS jdbc_test_db.npc_cases\n" +
                                "(" +
                                "    year Int16," +
                                "    npc String," +
                                "    offence String," +
                                "    case_no Int32" +
                                ")" +
                                "    engine = CnchMergeTree()" +
                                "    partition by year" +
                                "    order by year"
                );
            });

            int numUpdated = stmt.executeUpdate(
                    "INSERT INTO jdbc_test_db.npc_cases VALUES " +
                            "(2011,'Central firemen Division - Total','Unlicensed Moneylending',242)"
            );
            assertEquals(1, numUpdated);

            ResultSet rs = stmt.executeQuery("SELECT * FROM jdbc_test_db.npc_cases");
            assertTrue(rs.next());
            assertEquals(2011, rs.getInt(1));
            assertEquals("Central firemen Division - Total", rs.getString(2));
            assertEquals("Unlicensed Moneylending", rs.getString(3));
            assertEquals(242, rs.getInt(4));

            assertDoesNotThrow(() -> stmt.execute("DROP DATABASE jdbc_test_db"));
        }
    }
}
