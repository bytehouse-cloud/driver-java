/*
 * This file may have been modified by ByteDance Ltd. and/or its affiliates.
 *
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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.bytedance.bytehouse.exception.ByteHouseSQLException;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.ZoneId;
import org.junit.jupiter.api.Test;

public class PreparedStatementITest extends AbstractITest {

//    protected static final ZoneId CLIENT_TZ = ZoneId.of("UTC+8");
//    protected static final ZoneId SERVER_TZ = ZoneId.of("UTC+8");

    protected static final ZoneId CLIENT_TZ = ZoneId.systemDefault();
    protected static final ZoneId SERVER_TZ = ZoneId.systemDefault();

    @Test
    public void successfullyInt8Query() throws Exception {
        withPreparedStatement("SELECT ?,?", pstmt -> {
            pstmt.setByte(1, (byte) 1);
            pstmt.setByte(2, (byte) 2);
            ResultSet rs = pstmt.executeQuery();
            assertTrue(rs.next());
            assertEquals(1, rs.getByte(1));
            assertEquals(2, rs.getByte(2));
            assertFalse(rs.next());
        });
    }

    @Test
    public void successfullyInt16Query() throws Exception {
        withPreparedStatement("SELECT ?,?", pstmt -> {
            pstmt.setShort(1, (short) 1);
            pstmt.setShort(2, (short) 2);
            ResultSet rs = pstmt.executeQuery();
            assertTrue(rs.next());
            assertEquals(1, rs.getShort(1));
            assertEquals(2, rs.getShort(2));
            assertFalse(rs.next());
        });
    }

    @Test
    public void successfullyInt32Query() throws Exception {
        withPreparedStatement("SELECT ?,?", pstmt -> {
            pstmt.setInt(1, 1);
            pstmt.setInt(2, 2);
            ResultSet rs = pstmt.executeQuery();
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertEquals(2, rs.getInt(2));
            assertFalse(rs.next());
        });
    }

    @Test
    public void successfullyInt64Query() throws Exception {
        withPreparedStatement("SELECT ?,?", pstmt -> {
            pstmt.setLong(1, 1);
            pstmt.setLong(2, 2);
            ResultSet rs = pstmt.executeQuery();
            assertTrue(rs.next());
            assertEquals(1, rs.getLong(1));
            assertEquals(2, rs.getLong(2));
            assertFalse(rs.next());
        });
    }

    @Test
    public void successfullyStringQuery() throws Exception {
        withPreparedStatement("SELECT ?,?", pstmt -> {
            pstmt.setString(1, "test1");
            pstmt.setString(2, "test2");
            ResultSet rs = pstmt.executeQuery();
            assertTrue(rs.next());
            assertEquals("test1", rs.getString(1));
            assertEquals("test2", rs.getString(2));
            assertFalse(rs.next());
        });
    }

    @Test
    public void successfullyNullable() throws Exception {
        withPreparedStatement("SELECT arrayJoin([?,?])", pstmt -> {
            pstmt.setString(1, null);
            pstmt.setString(2, "test2");
            ResultSet rs = pstmt.executeQuery();
            assertTrue(rs.next());
            assertNull(rs.getString(1));
            assertTrue(rs.wasNull());
            assertTrue(rs.next());
            assertEquals("test2", rs.getString(1));
            assertFalse(rs.next());
        });
    }

    @Test
    public void checkBooleanNotSupported() throws Exception {
        withStatement(stmt -> {
            String databaseName = getDatabaseName();
            String tableName = databaseName + "." + getTableName();

            try {
                stmt.execute(String.format("CREATE DATABASE %s", databaseName));
                stmt.execute(String.format("CREATE TABLE %s (flag Boolean) ENGINE=CnchMergeTree() order by tuple()", tableName));
            } catch (SQLException e) {
                assertTrue(e instanceof ByteHouseSQLException);
            }
            finally {
                stmt.execute(String.format("DROP DATABASE %s", databaseName));
            }
        });
    }

    @Test
    public void testMetadata() throws Exception {
        withStatement(statement -> {
            String databaseName = getDatabaseName();
            String tableName = databaseName + "." + getTableName();

            try {
                statement.execute(String.format("CREATE DATABASE %s", databaseName));
                statement.execute(String.format("CREATE TABLE %s(id Int32)"
                        + " ENGINE=CnchMergeTree() order by tuple()", tableName));


                String preparedStatementInsertSql = String.format("INSERT INTO %s VALUES (?)", tableName);
                PreparedStatement preparedStatement = statement.getConnection().prepareStatement(preparedStatementInsertSql);

                ResultSetMetaData metaData = preparedStatement.getMetaData();
                assertEquals(metaData.getColumnCount(), 1);
                assertEquals(metaData.getColumnName(1), "id");

                preparedStatement.executeBatch();
            }
            finally {
                statement.execute(String.format("DROP DATABASE %s", databaseName));
                statement.getConnection().close();
            }
        });
    }

    @Test
    public void testPlaceholderWithValues() throws Exception {
        withStatement(statement -> {
            String databaseName = getDatabaseName();
            String tableName = databaseName + "." + getTableName();

            try {
                statement.execute(String.format("CREATE DATABASE %s", databaseName));
                statement.execute(String.format("CREATE TABLE %s(date Date, datetime DateTime, uuid UUID, int Int32, date2 Date)"
                        + " ENGINE=CnchMergeTree() order by tuple()", tableName));


                String preparedStatementInsertSql = String.format("INSERT INTO %s VALUES (?, '2012-01-01 00:00:00', ?, 5, ?)", tableName);
                PreparedStatement preparedStatement = statement.getConnection().prepareStatement(preparedStatementInsertSql);

                preparedStatement.setObject(1, Date.valueOf(LocalDate.of(2012, 9, 9)));
                preparedStatement.setObject(2, "23bdba3e-7d0b-41f9-a555-e6b4eb5e3f0b");
                preparedStatement.setObject(3, Date.valueOf(LocalDate.of(2025, 9, 10)));
                preparedStatement.addBatch();

                preparedStatement.executeBatch();
            }
            finally {
                statement.execute(String.format("DROP DATABASE %s", databaseName));
            }
        });
    }
}
