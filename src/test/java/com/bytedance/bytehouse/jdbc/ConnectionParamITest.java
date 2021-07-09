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

import com.bytedance.bytehouse.settings.ByteHouseConfig;
import com.bytedance.bytehouse.settings.SettingKey;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Locale;

import static org.junit.jupiter.api.Assertions.*;

public class ConnectionParamITest extends AbstractITest {

    @BeforeEach
    public void init() throws SQLException {
        resetDriverManager();
    }

    @Test
    public void successfullyMaxRowsToRead() {
        assertThrows(SQLException.class, () -> {
            try (Connection connection = DriverManager
                    .getConnection(String.format(Locale.ROOT, "jdbc:bytehouse://%s:%s?max_rows_to_read=1&connect_timeout=10", CK_HOST, CK_PORT))) {
                withStatement(connection, stmt -> {
                    ResultSet rs = stmt.executeQuery("SELECT arrayJoin([1,2,3,4]) from numbers(100)");
                    int rowsRead = 0;
                    while (rs.next()) {
                        ++rowsRead;
                    }
                    assertEquals(1, rowsRead); // not reached
                });
            }
        });
    }

    @Test
    public void successfullyMaxResultRows() throws Exception {
        try (Connection connection = DriverManager
                .getConnection(String.format(Locale.ROOT, "jdbc:bytehouse://%s:%s?max_result_rows=1&connect_timeout=10", CK_HOST, CK_PORT))
        ) {
            withStatement(connection, stmt -> {
                stmt.setMaxRows(400);
                ResultSet rs = stmt.executeQuery("SELECT arrayJoin([1,2,3,4]) from numbers(100)");
                int rowsRead = 0;
                while (rs.next()) {
                    ++rowsRead;
                }
                assertEquals(400, rowsRead);
            });
        }
    }

    @Test
    public void successfullyUrlParser() {
        String url = "jdbc:bytehouse://127.0.0.1/system?min_insert_block_size_rows=1000&connect_timeout=50";
        ByteHouseConfig config = ByteHouseConfig.Builder.builder().withJdbcUrl(url).build();
        assertEquals("system", config.database());
        assertEquals(1000L, config.settings().get(SettingKey.min_insert_block_size_rows));

        assertEquals(Duration.ofSeconds(50), config.connectTimeout());
    }

    @Test
    public void successfullyHostNameOnly() {
        String url = "jdbc:bytehouse://my_bytehouse_sever_host_name/system?min_insert_block_size_rows=1000&connect_timeout=50";
        ByteHouseConfig config = ByteHouseConfig.Builder.builder().withJdbcUrl(url).build();
        assertEquals("my_bytehouse_sever_host_name", config.host());
        assertEquals(9000, config.port());
        assertEquals("system", config.database());
        assertEquals(1000L, config.settings().get(SettingKey.min_insert_block_size_rows));
        assertEquals(Duration.ofSeconds(50), config.connectTimeout());
    }

    @Test
    public void successfullyHostNameWithDefaultPort() {
        String url = "jdbc:bytehouse://my_bytehouse_sever_host_name:9000/system?min_insert_block_size_rows=1000&connect_timeout=50";
        ByteHouseConfig config = ByteHouseConfig.Builder.builder().withJdbcUrl(url).build();
        assertEquals("my_bytehouse_sever_host_name", config.host());
        assertEquals(9000, config.port());
        assertEquals("system", config.database());
        assertEquals(1000L, config.settings().get(SettingKey.min_insert_block_size_rows));
        assertEquals(Duration.ofSeconds(50), config.connectTimeout());
    }

    @Test
    public void successfullyHostNameWithCustomPort() {
        String url = "jdbc:bytehouse://my_bytehouse_sever_host_name:1940/system?min_insert_block_size_rows=1000&connect_timeout=50";
        ByteHouseConfig config = ByteHouseConfig.Builder.builder().withJdbcUrl(url).build();
        assertEquals("my_bytehouse_sever_host_name", config.host());
        assertEquals(1940, config.port());
        assertEquals("system", config.database());
        assertEquals(1000L, config.settings().get(SettingKey.min_insert_block_size_rows));
        assertEquals(Duration.ofSeconds(50), config.connectTimeout());
    }

    @Test
    public void successfullyCompressedInsert() throws Exception {
        withStatement(statement -> {
            statement.execute("DROP TABLE IF EXISTS test");
            statement.execute("CREATE TABLE test(x Int32) ENGINE=Log");

            for (int i = 0; i < 30; i++) {
                assertEquals(1, statement.executeUpdate(String.format("INSERT INTO test VALUES(%d)", i)));
            }

            ResultSet rs = statement.executeQuery("SELECT x FROM test ORDER BY x");
            for (int i = 0; i < 30; i++) {
                assertTrue(rs.next());
                assertEquals(i, rs.getInt(1));
            }
            assertFalse(rs.next());

            statement.execute("DROP TABLE IF EXISTS test");
        }, "enable_compression", "true");
    }

    @Test
    public void successfullyCompressedInsertBatch() throws Exception {
        withStatement(statement -> {
            statement.execute("DROP TABLE IF EXISTS test");
            statement.execute("CREATE TABLE test(x Int32) ENGINE=Log");

            withPreparedStatement(statement.getConnection(), "INSERT INTO test(x) VALUES(?)", pstmt -> {
                for (int i = 0; i < 30; i++) {
                    pstmt.setInt(1, i);
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            });

            ResultSet rs = statement.executeQuery("SELECT x FROM test ORDER BY x");
            for (int i = 0; i < 30; i++) {
                assertTrue(rs.next());
                assertEquals(i, rs.getInt(1));
            }
            assertFalse(rs.next());

            statement.execute("DROP TABLE IF EXISTS test");
        }, "enable_compression", "true");
    }

    @Test
    public void successfullyCompressedQuery() throws Exception {
        withStatement(statement -> {
            ResultSet rs = statement.executeQuery("SELECT * FROM numbers(30)");
            for (int i = 0; i < 30; i++) {
                assertTrue(rs.next());
                assertEquals(i, rs.getInt(1));
            }
            assertFalse(rs.next());
        }, "enable_compression", "true");
    }
}
