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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import org.junit.jupiter.api.Test;

public class ByteHouseConnectionITest extends AbstractITest {

    @Test
    public void testCatalog() throws Exception {
        withNewConnection(connection -> {
            assertNull(connection.getCatalog());
            connection.setCatalog("abc");
            assertNull(connection.getCatalog());
        });
    }

    @Test
    public void testSchema() throws Exception {
        withNewConnection(connection -> {
            assertEquals("", connection.getSchema());
            connection.setSchema("abc");
            assertEquals("abc", connection.getSchema());
        });
    }

    @Test
    public void testTransactionNotSupported() throws Exception {
        withNewConnection(connection -> {
            assertTrue(connection.getAutoCommit());
            assertThrows(SQLFeatureNotSupportedException.class, () -> connection.setAutoCommit(false));
            connection.setAutoCommit(true);
            assertThrows(SQLException.class, connection::commit);
            assertThrows(SQLException.class, connection::rollback);
            assertThrows(SQLException.class,
                    () -> connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED));
            assertEquals(connection.getTransactionIsolation(), Connection.TRANSACTION_NONE);
        });
    }

    @Test
    public void testReadOnlyNotSupported() throws Exception {
        withNewConnection(connection -> {
            assertFalse(connection.isReadOnly());
            connection.setReadOnly(false);
            assertThrows(SQLFeatureNotSupportedException.class, () -> connection.setReadOnly(true));
        });
    }

    @Test
    public void testHoldability() throws Exception {
        withNewConnection(connection -> {
            assertEquals(connection.getHoldability(), ResultSet.CLOSE_CURSORS_AT_COMMIT);
            assertThrows(SQLFeatureNotSupportedException.class,
                    () -> connection.setHoldability(ResultSet.CONCUR_READ_ONLY));
        });
    }

    @Test
    public void testSetClientInfo() throws Exception {
        withNewConnection(connection -> {
            assertThrows(SQLClientInfoException.class,
                    () -> connection.setClientInfo("ApplicationName", "BH"));
        });
    }

    @Test
    public void testIsValidAndClose() throws Exception {
        withNewConnection(connection -> {
            assertFalse(connection.isClosed());
            assertTrue(connection.isValid(1));
            connection.close();
            assertTrue(connection.isClosed());
            assertFalse(connection.isValid(1));
        });
    }

    @Test
    public void testNewConnectionPreparedStatement() throws Exception {
        withStatement(statement -> {
            String databaseName = getDatabaseName();
            String tableName = databaseName + "." + getTableName();

            try {
                statement.execute(String.format("CREATE DATABASE %s", databaseName));
                statement.execute(String.format("CREATE TABLE %s(id String) ENGINE=CnchMergeTree() order by tuple()", tableName));

                PreparedStatement pstmt = getConnection().prepareStatement(String.format("INSERT INTO %s VALUES (?)", tableName));
                pstmt.setString(1, "id-01");
                pstmt.executeBatch();
            }
            finally {
                statement.execute(String.format("DROP DATABASE %s", databaseName));
            }
        });
    }
}
