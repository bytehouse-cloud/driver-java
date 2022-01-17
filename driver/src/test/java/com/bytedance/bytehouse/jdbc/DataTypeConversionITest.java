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
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DataTypeConversionITest extends AbstractITest {

    void assertBatchInsertResult(int[] result, int expectedRowCount) {
        assertEquals(expectedRowCount, result.length);
        assertEquals(expectedRowCount, Arrays.stream(result).sum());
    }

    @Test
    public void testStringToInt32() throws Exception {
        withStatement(statement -> {
            String databaseName = getDatabaseName();
            String tableName = databaseName + "." + getTableName();

            try {
                statement.execute(String.format("CREATE DATABASE %s", databaseName));
                statement.execute(String.format("CREATE TABLE %s(id Int32)"
                        + " ENGINE=CnchMergeTree() order by tuple()", tableName));

                withPreparedStatement(String.format("INSERT INTO %s VALUES(?)", tableName), pstmt -> {
                    for (int i = 0; i < Byte.MAX_VALUE; i++) {
                        pstmt.setObject(1, String.valueOf(i+1));
                        pstmt.addBatch();
                    }
                    assertBatchInsertResult(pstmt.executeBatch(), Byte.MAX_VALUE);
                });
                ResultSet rs = statement.executeQuery(String.format("select * from %s", tableName));
                boolean hasResult = false;
                for (int i = 0; i < Byte.MAX_VALUE && rs.next(); i++) {
                    hasResult = true;
                    assertEquals(i+1, rs.getInt(1));
                }
                assertTrue(hasResult);
            }
            finally {
                statement.execute(String.format("DROP DATABASE %s", databaseName));
            }
        });
    }

    @Test
    public void testStringToUInt8() throws Exception {
        withStatement(statement -> {
            String databaseName = getDatabaseName();
            String tableName = databaseName + "." + getTableName();

            try {
                statement.execute(String.format("CREATE DATABASE %s", databaseName));
                statement.execute(String.format("CREATE TABLE %s(id UInt8)"
                        + " ENGINE=CnchMergeTree() order by tuple()", tableName));

                withPreparedStatement(String.format("INSERT INTO %s VALUES(?)", tableName), pstmt -> {
                    for (int i = 0; i < Byte.MAX_VALUE; i++) {
                        pstmt.setObject(1, String.valueOf(i+1));
                        pstmt.addBatch();
                    }
                    assertBatchInsertResult(pstmt.executeBatch(), Byte.MAX_VALUE);
                });
                ResultSet rs = statement.executeQuery(String.format("select * from %s", tableName));
                boolean hasResult = false;
                for (int i = 0; i < Byte.MAX_VALUE && rs.next(); i++) {
                    hasResult = true;
                    assertEquals(i+1, rs.getShort(1));
                }
                assertTrue(hasResult);
            }
            finally {
                statement.execute(String.format("DROP DATABASE %s", databaseName));
            }
        });
    }

    @Test
    public void testStringToDate() throws Exception {
        withStatement(statement -> {
            String databaseName = getDatabaseName();
            String tableName = databaseName + "." + getTableName();

            try {
                statement.execute(String.format("CREATE DATABASE %s", databaseName));
                statement.execute(String.format("CREATE TABLE %s(id Date)"
                        + " ENGINE=CnchMergeTree() order by tuple()", tableName));

                withPreparedStatement(String.format("INSERT INTO %s VALUES(?)", tableName), pstmt -> {
                    pstmt.setObject(1, "1999-05-05");
                    pstmt.addBatch();
                    pstmt.executeBatch();
                });
                ResultSet rs = statement.executeQuery(String.format("select * from %s", tableName));
                assertTrue(rs.next());
                assertEquals(rs.getObject(1), Date.valueOf(LocalDate.of(1999, 05, 05)));
            }
            finally {
                statement.execute(String.format("DROP DATABASE %s", databaseName));
            }
        });
    }

    @Test
    public void testStringToBigDecimal() throws Exception {
        withStatement(statement -> {
            String databaseName = getDatabaseName();
            String tableName = databaseName + "." + getTableName();

            try {
                statement.execute(String.format("CREATE DATABASE %s", databaseName));
                statement.execute(String.format("CREATE TABLE %s(id Decimal(18,2))"
                        + " ENGINE=CnchMergeTree() order by tuple()", tableName));

                withPreparedStatement(String.format("INSERT INTO %s VALUES(?)", tableName), pstmt -> {
                    pstmt.setObject(1, "1265.00");
                    pstmt.addBatch();
                    pstmt.executeBatch();
                });
                ResultSet rs = statement.executeQuery(String.format("select * from %s", tableName));
                assertTrue(rs.next());
                assertEquals(Double.valueOf(rs.getObject(1).toString()), Double.valueOf("1265.0"));
            }
            finally {
                statement.execute(String.format("DROP DATABASE %s", databaseName));
            }
        });
    }

    @Test
    public void testStringToDateTime() throws Exception {
        withStatement(statement -> {
            String databaseName = getDatabaseName();
            String tableName = databaseName + "." + getTableName();

            try {
                statement.execute(String.format("CREATE DATABASE %s", databaseName));
                statement.execute(String.format("CREATE TABLE %s(id Datetime)"
                        + " ENGINE=CnchMergeTree() order by tuple()", tableName));

                withPreparedStatement(String.format("INSERT INTO %s VALUES(?)", tableName), pstmt -> {
                    pstmt.setObject(1, "2021-04-01T00:00:00");
                    pstmt.addBatch();
                    pstmt.executeBatch();
                });
                ResultSet rs = statement.executeQuery(String.format("select * from %s", tableName));
                assertTrue(rs.next());
                assertEquals(rs.getTimestamp(1), Timestamp.valueOf(LocalDateTime.of(2021, 4, 1, 0, 0, 0, 0)));
            }
            finally {
                statement.execute(String.format("DROP DATABASE %s", databaseName));
            }
        });
    }

    @Test
    public void testStringToUInt16() throws Exception {
        withStatement(statement -> {
            String databaseName = getDatabaseName();
            String tableName = databaseName + "." + getTableName();

            try {
                statement.execute(String.format("CREATE DATABASE %s", databaseName));
                statement.execute(String.format("CREATE TABLE %s(id UInt16)"
                        + " ENGINE=CnchMergeTree() order by tuple()", tableName));

                withPreparedStatement(String.format("INSERT INTO %s VALUES(?)", tableName), pstmt -> {
                    pstmt.setObject(1, "25899");
                    pstmt.addBatch();
                    pstmt.executeBatch();
                });
                ResultSet rs = statement.executeQuery(String.format("select * from %s", tableName));
                assertTrue(rs.next());
                assertEquals(rs.getInt(1), 25899);
            }
            finally {
                statement.execute(String.format("DROP DATABASE %s", databaseName));
            }
        });
    }
}
