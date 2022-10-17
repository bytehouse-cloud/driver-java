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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.time.LocalDate;
import org.junit.jupiter.api.Test;

public class InsertSimpleTypeITest extends AbstractITest {

    @Test
    public void successfullyInt8DataType() throws Exception {
        String databaseName = getDatabaseName();
        String tableName = databaseName + "." + getTableName();

        withStatement(statement -> {
            try {
                statement.execute(String.format("CREATE DATABASE %s", databaseName));
                statement.execute(String.format("CREATE TABLE %s (test_uInt8 UInt8, test_Int8 Int8)ENGINE=CnchMergeTree() order by tuple()", tableName));

                statement.executeQuery(String.format("INSERT INTO %s VALUES(" + 255 + "," + Byte.MIN_VALUE + ")", tableName));

                ResultSet rs = statement.executeQuery(String.format("SELECT * FROM %s ORDER BY test_uInt8", tableName));
                assertTrue(rs.next());
                int aa = rs.getInt(1);
                assertEquals(255, aa);
                assertEquals(Byte.MIN_VALUE, rs.getByte(2));
                assertFalse(rs.next());
            }
            finally {
                statement.execute(String.format("DROP DATABASE %s", databaseName));
            }
        });
    }

    @Test
    public void successfullyInt16DataType() throws Exception {
        String databaseName = getDatabaseName();
        String tableName = databaseName + "." + getTableName();

        withStatement(statement -> {
            try {
                statement.execute(String.format("CREATE DATABASE %s", databaseName));
                statement.execute(String.format("CREATE TABLE %s (test_uInt16 UInt16, test_Int16 Int16)ENGINE=CnchMergeTree() order by tuple()", tableName));

                statement.executeQuery(String.format("INSERT INTO %s VALUES(" + Short.MAX_VALUE + "," + Short.MIN_VALUE + ")", tableName));
                ResultSet rs = statement.executeQuery(String.format("SELECT * FROM %s ORDER BY test_uInt16", tableName));
                assertTrue(rs.next());
                assertEquals(Short.MAX_VALUE, rs.getShort(1));
                assertEquals(Short.MIN_VALUE, rs.getShort(2));
                assertFalse(rs.next());
            }
            finally {
                statement.execute(String.format("DROP DATABASE %s", databaseName));
            }
        });
    }

    @Test
    public void successfullyInt32DataType() throws Exception {
        String databaseName = getDatabaseName();
        String tableName = databaseName + "." + getTableName();

        withStatement(statement -> {
            try {
                statement.execute(String.format("CREATE DATABASE %s", databaseName));
                statement.execute(String.format("CREATE TABLE %s (test_uInt32 UInt32, test_Int32 Int32)ENGINE=CnchMergeTree() order by tuple()", tableName));

                statement.executeQuery(String.format("INSERT INTO %s VALUES(" + Integer.MAX_VALUE + "," + Integer.MIN_VALUE + ")", tableName));
                ResultSet rs = statement.executeQuery(String.format("SELECT * FROM %s ORDER BY test_uInt32", tableName));
                assertTrue(rs.next());
                assertEquals(Integer.MAX_VALUE, rs.getInt(1));
                assertEquals(Integer.MIN_VALUE, rs.getInt(2));
                assertFalse(rs.next());
            }
            finally {
                statement.execute(String.format("DROP DATABASE %s", databaseName));
            }
        });
    }

    @Test
    public void successfullyIPv4DataType() throws Exception {
        String databaseName = getDatabaseName();
        String tableName = databaseName + "." + getTableName();

        withStatement(statement -> {
            try {
                statement.execute(String.format("CREATE DATABASE %s", databaseName));
                statement.execute(String.format("CREATE TABLE %s (min_ip IPv4,max_ip IPv4)ENGINE=CnchMergeTree() order by tuple()", tableName));

                long minIp = 0L;
                long maxIp = (1L << 32) - 1;

                withPreparedStatement(getConnection(), String.format("INSERT INTO %s (min_ip, max_ip) VALUES(?, ?)", tableName), pstmt -> {
                    for (int i = 0; i < 1; i++) {
                        pstmt.setLong(1, minIp);
                        pstmt.setLong(2, maxIp);
                        pstmt.addBatch();
                    }
                    pstmt.executeBatch();
                });
                ResultSet rs = statement.executeQuery(String.format("SELECT min_ip,max_ip FROM %s", tableName));
                assertTrue(rs.next());
                assertEquals(minIp, rs.getLong(1));
                assertEquals(maxIp, rs.getLong(2));
                assertFalse(rs.next());
            }
            finally {
                statement.execute(String.format("DROP DATABASE %s", databaseName));
            }
        });
    }

    @Test
    public void successfullyIPv6DataType() throws Exception {
        String databaseName = getDatabaseName();
        String tableName = databaseName + "." + getTableName();

        withStatement(statement -> {
            try {
                statement.execute(String.format("CREATE DATABASE %s", databaseName));
                statement.execute(String.format("CREATE TABLE %s (ip IPv6)ENGINE=CnchMergeTree() order by tuple()", tableName));


                String testIp = "2001:44c8:129:2632:33:0:252:2";
                statement.executeQuery(String.format("INSERT INTO %s VALUES('%s')", tableName, testIp));

                ResultSet rs = statement.executeQuery(String.format("SELECT ip FROM %s", tableName));

                assertTrue(rs.next());
                assertEquals(rs.getString(1), "/" + testIp);
            }
            finally {
                statement.execute(String.format("DROP DATABASE %s", databaseName));
            }
        });
    }

    @Test
    public void successfullyIPv6DataTypeBatch() throws Exception {
        String databaseName = getDatabaseName();
        String tableName = databaseName + "." + getTableName();

        withStatement(statement -> {
            try {
                statement.execute(String.format("CREATE DATABASE %s", databaseName));
                statement.execute(String.format("CREATE TABLE %s (ip IPv6)ENGINE=CnchMergeTree() order by tuple()", tableName));


                String[] testIps = {"0:0:0:0:0:0:0:0", "2001:44c8:129:2632:33:0:252:2", "2a02:aa08:e000:3100::2"};
                String[] testIpsOutput = {
                        "/0:0:0:0:0:0:0:0", "/2001:44c8:129:2632:33:0:252:2", "/2a02:aa08:e000:3100:0:0:0:2"
                };

                withPreparedStatement(getConnection(), String.format("INSERT INTO %s(ip) VALUES(?)", tableName), pstmt -> {
                    for (String testIp : testIps) {
                        pstmt.setString(1, testIp);
                        pstmt.addBatch();
                    }
                    pstmt.executeBatch();
                });

                ResultSet rs = statement.executeQuery(String.format("SELECT ip FROM %s", tableName));

                for (int i = 0; i < testIps.length; i++) {
                    assertTrue(rs.next());
                    assertEquals(rs.getString(1), testIpsOutput[i]);
                }
                assertFalse(rs.next());
            }
            finally {
                statement.execute(String.format("DROP DATABASE %s", databaseName));
            }
        });
    }

    @Test
    public void successfullyInt64DataType() throws Exception {
        String databaseName = getDatabaseName();
        String tableName = databaseName + "." + getTableName();

        withStatement(statement -> {
            try {
                statement.execute(String.format("CREATE DATABASE %s", databaseName));
                statement.execute(String.format("CREATE TABLE %s (test_uInt64 UInt64, test_Int64 Int64)ENGINE=CnchMergeTree() order by tuple()", tableName));

                statement.executeQuery(String.format("INSERT INTO %s VALUES(" + Long.MAX_VALUE + "," + Long.MIN_VALUE + ")", tableName));
                ResultSet rs = statement.executeQuery(String.format("SELECT * FROM %s ORDER BY test_uInt64", tableName));
                assertTrue(rs.next());
                assertEquals(Long.MAX_VALUE, rs.getLong(1));
                assertEquals(Long.MIN_VALUE, rs.getLong(2));
                assertFalse(rs.next());
            }
            finally {
                statement.execute(String.format("DROP DATABASE %s", databaseName));
            }
        });
    }

    @Test
    public void successfullyStringDataType() throws Exception {
        String databaseName = getDatabaseName();
        String tableName = databaseName + "." + getTableName();

        withStatement(statement -> {
            try {
                statement.execute(String.format("CREATE DATABASE %s", databaseName));
                statement.execute(String.format("CREATE TABLE %s (test String)ENGINE=CnchMergeTree() order by tuple()", tableName));

                statement.executeQuery(String.format("INSERT INTO %s VALUES('我爱祖国')", tableName));
                ResultSet rs = statement.executeQuery(String.format("SELECT * FROM %s", tableName));
                assertTrue(rs.next());
                assertEquals("我爱祖国", rs.getString(1));
                assertFalse(rs.next());
            }
            finally {
                statement.execute(String.format("DROP DATABASE %s", databaseName));
            }
        });
    }

    @Test
    public void successfullyDateDataType() throws Exception {
        String databaseName = getDatabaseName();
        String tableName = databaseName + "." + getTableName();

        withStatement(statement -> {
            try {
                statement.execute(String.format("CREATE DATABASE %s", databaseName));
                statement.execute(String.format("CREATE TABLE %s (test Date, test2 Date)ENGINE=CnchMergeTree() order by tuple()", tableName));

                statement.executeQuery(String.format("INSERT INTO %s VALUES('2000-01-01', '2000-01-31')", tableName));
                ResultSet rs = statement.executeQuery(String.format("SELECT * FROM %s", tableName));
                assertTrue(rs.next());
                assertEquals(
                        LocalDate.of(2000, 1, 1).toEpochDay(),
                        rs.getDate(1).toLocalDate().toEpochDay());
                assertEquals(
                        LocalDate.of(2000, 1, 31).toEpochDay(),
                        rs.getDate(2).toLocalDate().toEpochDay());

                assertFalse(rs.next());
            }
            finally {
                statement.execute(String.format("DROP DATABASE %s", databaseName));
            }
        }, "use_client_time_zone", true);
    }

    @Test
    public void successfullyFloatDataType() throws Exception {
        String databaseName = getDatabaseName();
        String tableName = databaseName + "." + getTableName();

        withStatement(statement -> {
            try {
                statement.execute(String.format("CREATE DATABASE %s", databaseName));
                statement.execute(String.format("CREATE TABLE %s (test_float32 Float32)ENGINE=CnchMergeTree() order by tuple()", tableName));

                statement.executeQuery(String.format("INSERT INTO %s VALUES(" + Float.MIN_VALUE + ")(" + Float.MAX_VALUE + ")", tableName));
                ResultSet rs = statement.executeQuery(String.format("SELECT * FROM %s ORDER BY test_float32", tableName));
                assertTrue(rs.next());
                assertEquals(Float.MIN_VALUE, rs.getFloat(1), 0.000000000001);
                assertTrue(rs.next());
                assertEquals(Float.MAX_VALUE, rs.getFloat(1), 0.000000000001);
                assertFalse(rs.next());
            }
            finally {
                statement.execute(String.format("DROP DATABASE %s", databaseName));
            }
        });
    }


    @Test
    public void successfullyDoubleDataType() throws Exception {
        String databaseName = getDatabaseName();
        String tableName = databaseName + "." + getTableName();

        withStatement(statement -> {
            try {
                statement.execute(String.format("CREATE DATABASE %s", databaseName));
                statement.execute(String.format("CREATE TABLE %s (test_float64 Float64)ENGINE=CnchMergeTree() order by tuple()", tableName));

                statement.executeQuery(String.format("INSERT INTO %s VALUES(" + Double.MIN_VALUE + ")(" + Double.MAX_VALUE + ")", tableName));
                ResultSet rs = statement.executeQuery(String.format("SELECT * FROM %s ORDER BY test_float64", tableName));
                assertTrue(rs.next());
                assertEquals(Double.MIN_VALUE, rs.getDouble(1), 0.000000000001);
                assertTrue(rs.next());
                assertEquals(Double.MAX_VALUE, rs.getDouble(1), 0.000000000001);
                assertFalse(rs.next());
            }
            finally {
                statement.execute(String.format("DROP DATABASE %s", databaseName));
            }
        });
    }

    @Test
    public void successfullyUUIDDataType() throws Exception {
        String databaseName = getDatabaseName();
        String tableName = databaseName + "." + getTableName();

        withStatement(statement -> {
            try {
                statement.execute(String.format("CREATE DATABASE %s", databaseName));
                statement.execute(String.format("CREATE TABLE %s (test UUID)ENGINE=CnchMergeTree() order by tuple()", tableName));

                statement.executeQuery(String.format("INSERT INTO %s VALUES('01234567-89ab-cdef-0123-456789abcdef')", tableName));
                ResultSet rs = statement.executeQuery(String.format("SELECT * FROM %s", tableName));
                assertTrue(rs.next());
                assertEquals("01234567-89ab-cdef-0123-456789abcdef", rs.getString(1));
                assertFalse(rs.next());
            }
            finally {
                statement.execute(String.format("DROP DATABASE %s", databaseName));
            }
        });
    }

    @Test
    public void successfullyMultipleValuesWithComma() throws Exception {
        String databaseName = getDatabaseName();
        String tableName = databaseName + "." + getTableName();

        withStatement(statement -> {
            try {
                statement.execute(String.format("CREATE DATABASE %s", databaseName));
                statement.execute(String.format("CREATE TABLE %s (id Int32)ENGINE=CnchMergeTree() order by tuple()", tableName));

                statement.execute(String.format("INSERT INTO %s VALUES (1), (2)", tableName));
                ResultSet rs = statement.executeQuery(String.format("SELECT * FROM %s", tableName));
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1));
                assertFalse(rs.next());
            }
            finally {
                statement.execute(String.format("DROP DATABASE %s", databaseName));
            }
        });
    }

    @Test
    public void successfullyUnsignedDataType() throws Exception {
        String databaseName = getDatabaseName();
        String tableName = databaseName + "." + getTableName();

        withStatement(statement -> {
            try {
                statement.execute(String.format("CREATE DATABASE %s", databaseName));
                statement.execute(String.format("CREATE TABLE %s (i8 UInt8, i16 UInt16, i32 UInt32, i64 UInt64)ENGINE=CnchMergeTree() order by tuple()", tableName));

                String insertSQL = String.format("INSERT INTO %s VALUES(" + ((1 << 8) - 1) +
                        "," + ((1 << 16) - 1) +
                        ",4294967295,-9223372036854775808)", tableName);

                statement.executeQuery(insertSQL);

                ResultSet rs = statement.executeQuery(String.format("SELECT * FROM %s ORDER BY i8", tableName));
                assertTrue(rs.next());
                assertEquals(-1, rs.getByte(1));
                assertEquals((1 << 8) - 1, rs.getShort(1));

                assertEquals(-1, rs.getShort(2));
                assertEquals((1 << 16) - 1, rs.getInt(2));

                assertEquals(-1, rs.getInt(3));
                assertEquals(4294967295L, rs.getLong(3));

                assertEquals(-9223372036854775808L, rs.getLong(4));
                assertEquals(new BigDecimal("9223372036854775808"), rs.getBigDecimal(4));
                assertFalse(rs.next());
            }
            finally {
                statement.execute(String.format("DROP DATABASE %s", databaseName));
            }
        });
    }

    @Test
    public void successfullyCharset() throws Exception {
        String[] charsets = new String[]{"UTF-8", "GB2312", "UTF-16"};
        for (String charset : charsets) {
            withStatement(statement -> {
                String databaseName = getDatabaseName();
                String tableName = databaseName + "." + getTableName();

                try {
                    statement.execute(String.format("CREATE DATABASE %s", databaseName));
                    statement.execute(String.format("CREATE TABLE %s (s1 String, s2 String)ENGINE=CnchMergeTree() order by tuple()", tableName));

                    String insertSQL = String.format("INSERT INTO %s VALUES('" + "我爱中国" +
                            "','" + "我爱地球" +
                            "')", tableName);

                    statement.executeQuery(insertSQL);

                    ResultSet rs = statement.executeQuery(String.format("SELECT * FROM %s ORDER BY s1", tableName));
                    assertTrue(rs.next());
                    assertEquals("我爱中国", rs.getString(1));
                    assertEquals("我爱地球", rs.getString(2));

                    assertArrayEquals("我爱中国".getBytes(charset), rs.getBytes(1));
                    assertArrayEquals("我爱地球".getBytes(charset), rs.getBytes(2));

                    assertFalse(rs.next());
                }
                finally {
                    statement.execute(String.format("DROP DATABASE %s", databaseName));
                }
            }, "charset", charset);
        }
    }
}
