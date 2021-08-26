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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.bytedance.bytehouse.settings.ByteHouseConfig;
import com.bytedance.bytehouse.settings.SettingKey;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import org.junit.jupiter.api.Test;

public class ConnectionParamITest extends AbstractITest {

    @Test
    public void successfullyMaxRowsToRead() {
        assertThrows(SQLException.class, () -> {
            withStatement(statement -> {
                ResultSet rs = statement.executeQuery("SELECT arrayJoin([1,2,3,4]) from numbers(100)");
                int rowsRead = 0;
                while (rs.next()) {
                    ++rowsRead;
                }
                assertEquals(1, rowsRead); // not reached
            }, "max_rows_to_read", "1", "connect_timeout", "10");
        });
    }

    @Test
    public void successfullyMaxResultRows() throws Exception {
        withStatement(stmt -> {
            stmt.setMaxRows(400);
            ResultSet rs = stmt.executeQuery("SELECT arrayJoin([1,2,3,4]) from numbers(100)");
            int rowsRead = 0;
            while (rs.next()) {
                ++rowsRead;
            }
            assertEquals(400, rowsRead);
        }, "max_result_rows", "1", "connect_timeout", "10");
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
        String url = "jdbc:bytehouse://bytehouse-hostname/system?min_insert_block_size_rows=1000&connect_timeout=50";
        ByteHouseConfig config = ByteHouseConfig.Builder.builder().withJdbcUrl(url).build();
        assertEquals("bytehouse-hostname", config.host());
        assertEquals(9000, config.port());
        assertEquals("system", config.database());
        assertEquals(1000L, config.settings().get(SettingKey.min_insert_block_size_rows));
        assertEquals(Duration.ofSeconds(50), config.connectTimeout());
    }

    @Test
    public void successfullyHostNameWithDefaultPort() {
        String url = "jdbc:bytehouse://bytehouse-hostname:9000/system?min_insert_block_size_rows=1000&connect_timeout=50";
        ByteHouseConfig config = ByteHouseConfig.Builder.builder().withJdbcUrl(url).build();
        assertEquals("bytehouse-hostname", config.host());
        assertEquals(9000, config.port());
        assertEquals("system", config.database());
        assertEquals(1000L, config.settings().get(SettingKey.min_insert_block_size_rows));
        assertEquals(Duration.ofSeconds(50), config.connectTimeout());
    }

    @Test
    public void successfullyHostNameWithCustomPort() {
        String url = "jdbc:bytehouse://bytehouse-hostname:1940/system?min_insert_block_size_rows=1000&connect_timeout=50";
        ByteHouseConfig config = ByteHouseConfig.Builder.builder().withJdbcUrl(url).build();
        assertEquals("bytehouse-hostname", config.host());
        assertEquals(1940, config.port());
        assertEquals("system", config.database());
        assertEquals(1000L, config.settings().get(SettingKey.min_insert_block_size_rows));
        assertEquals(Duration.ofSeconds(50), config.connectTimeout());
    }

    @Test
    public void successfullyCompressedInsert() throws Exception {
        withStatement(statement -> {
            statement.execute("DROP DATABASE IF EXISTS test_db");
            statement.execute("CREATE DATABASE test_db");
            statement.execute("CREATE TABLE test_db.test_table (x Int32) ENGINE=CnchMergeTree() order by tuple()");

            for (int i = 0; i < 30; i++) {
                assertEquals(1, statement.executeUpdate(String.format("INSERT INTO test_db.test_table VALUES(%d)", i)));
            }

            ResultSet rs = statement.executeQuery("SELECT x FROM test_db.test_table ORDER BY x");
            for (int i = 0; i < 30; i++) {
                assertTrue(rs.next());
                assertEquals(i, rs.getInt(1));
            }
            assertFalse(rs.next());

            statement.execute("DROP DATABASE IF EXISTS test_db");
        }, "enable_compression", "true");
    }

    @Test
    public void successfullyCompressedInsertBatch() throws Exception {
        withStatement(statement -> {
            statement.execute("DROP DATABASE IF EXISTS test_db");
            statement.execute("CREATE DATABASE test_db");
            statement.execute("CREATE TABLE test_db.test_table (x Int32) ENGINE=CnchMergeTree() order by tuple()");


            withPreparedStatement(getConnection(), "INSERT INTO test_db.test_table(x) VALUES(?)", pstmt -> {
                for (int i = 0; i < 30; i++) {
                    pstmt.setInt(1, i);
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            });

            ResultSet rs = statement.executeQuery("SELECT x FROM test_db.test_table ORDER BY x");
            for (int i = 0; i < 30; i++) {
                assertTrue(rs.next());
                assertEquals(i, rs.getInt(1));
            }
            assertFalse(rs.next());

            statement.execute("DROP DATABASE IF EXISTS test_db");
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
