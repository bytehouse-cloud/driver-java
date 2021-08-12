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
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class BatchInsertITest extends AbstractITest {

    void assertBatchInsertResult(int[] result, int expectedRowCount) {
        assertEquals(expectedRowCount, result.length);
        assertEquals(expectedRowCount, Arrays.stream(result).sum());
    }

    @Test
    public void successfullyBatchInsert() throws Exception {
        withStatement(statement -> {
            statement.execute("DROP TABLE IF EXISTS test");
            statement.execute("CREATE TABLE test(id Int8, age UInt8, name String, name2 String)ENGINE=Log");

            withPreparedStatement(statement.getConnection(), "INSERT INTO test VALUES(?, 1, ?, ?)", pstmt -> {
                for (int i = 0; i < Byte.MAX_VALUE; i++) {
                    pstmt.setByte(1, (byte) i);
                    pstmt.setString(2, "Zhang San" + i);
                    pstmt.setString(3, "张三" + i);
                    pstmt.addBatch();
                }
                assertBatchInsertResult(pstmt.executeBatch(), Byte.MAX_VALUE);
            });
            ResultSet rs = statement.executeQuery("select * from test");
            boolean hasResult = false;
            for (int i = 0; i < Byte.MAX_VALUE && rs.next(); i++) {
                hasResult = true;
                assertEquals(i, rs.getByte(1));
                assertEquals(1, rs.getByte(2));
                assertEquals("Zhang San" + i, rs.getString(3));
                assertEquals("张三" + i, rs.getString(4));
            }
            assertTrue(hasResult);
        });

    }

    @Test
    public void successfullyMultipleBatchInsert() throws Exception {
        withStatement(statement -> {
            statement.execute("DROP TABLE IF EXISTS test");
            statement.execute("CREATE TABLE test(id Int8, age UInt8, name String)ENGINE=Log");

            withPreparedStatement(statement.getConnection(), "INSERT INTO test VALUES(?, 1, ?)", pstmt -> {
                int insertBatchSize = 100;

                for (int i = 0; i < insertBatchSize; i++) {
                    pstmt.setByte(1, (byte) i);
                    pstmt.setString(2, "Zhang San" + i);
                    pstmt.addBatch();
                }
                assertBatchInsertResult(pstmt.executeBatch(), insertBatchSize);

                for (int i = 0; i < insertBatchSize; i++) {
                    pstmt.setByte(1, (byte) i);
                    pstmt.setString(2, "Zhang San" + i);
                    pstmt.addBatch();
                }
                assertBatchInsertResult(pstmt.executeBatch(), insertBatchSize);
            });
        });

    }

    @Test
    public void successfullyNullableDataType() throws Exception {
        withStatement(stmt -> {
            int insertBatchSize = 100;

            stmt.executeQuery("DROP TABLE IF EXISTS test");
            stmt.executeQuery("CREATE TABLE test(day Date, name Nullable(String), name2 Nullable(FixedString(10)) ) Engine=Memory");

            withPreparedStatement(stmt.getConnection(), "INSERT INTO test VALUES(?, ?, ?)", pstmt -> {
                for (int i = 0; i < insertBatchSize; i++) {
                    pstmt.setDate(1, new Date(System.currentTimeMillis()));

                    if (i % 2 == 0) {
                        pstmt.setString(2, "String");
                        pstmt.setString(3, "String");
                    } else {
                        pstmt.setString(2, null);
                        pstmt.setString(3, null);
                    }
                    pstmt.addBatch();
                }
                assertBatchInsertResult(pstmt.executeBatch(), insertBatchSize);
            });
            ResultSet rs = stmt.executeQuery("select name, name2 from test order by name");
            int i = 0;
            while (rs.next()) {
                String name1 = rs.getString(1);
                String name2 = rs.getString(2);

                if (i * 2 >= insertBatchSize) {
                    assertNull(name1);
                } else {
                    assertEquals("String", name1);
                    assertTrue(name2.contains("String"));
                    assertEquals(10, name2.length());
                }
                i++;
            }

            rs = stmt.executeQuery(
                    "select countIf(isNull(name)), countIf(isNotNull(name)), countIf(isNotNull(name2))  from test;");
            assertTrue(rs.next());
            assertEquals(insertBatchSize / 2, rs.getInt(1));
            assertEquals(insertBatchSize / 2, rs.getInt(2));
            assertEquals(insertBatchSize / 2, rs.getInt(3));

            stmt.executeQuery("DROP TABLE IF EXISTS test");
        });
    }

    @Test
    public void successfullyBatchInsertArray() throws Exception {
        System.setProperty("illegal-access", "allow");

        withStatement(statement -> {
            statement.execute("DROP TABLE IF EXISTS test");
            statement.execute("CREATE TABLE test(value0 Array(String), value1 Array(Float64), value2 Array(Array(Int32)), array3 Array(Nullable(Float64)))ENGINE=Log");

            withPreparedStatement(statement.getConnection(), "INSERT INTO test VALUES(?, ?, [[1,2,3]], ?)", pstmt -> {
                List<String> array0 = Arrays.asList("aa", "bb", "cc");
                List<Double> array1 = Arrays.asList(1.2, 2.2, 3.2);
                List<Double> array3 = Arrays.asList(1.2, 2.2, 3.2, null);

                for (int i = 0; i < Byte.MAX_VALUE; i++) {
                    pstmt.setArray(1, pstmt.getConnection().createArrayOf("String", array0.toArray()));
                    pstmt.setArray(2, pstmt.getConnection().createArrayOf("Float64", array1.toArray()));
                    pstmt.setArray(3, pstmt.getConnection().createArrayOf("Nullable(Float64)", array3.toArray()));
                    pstmt.addBatch();
                }

                assertBatchInsertResult(pstmt.executeBatch(), Byte.MAX_VALUE);

                ResultSet rs = statement.executeQuery("select * from test");
                while (rs.next()) {
                    assertArrayEquals(array0.toArray(), (Object[]) rs.getArray(1).getArray());
                    assertArrayEquals(array1.toArray(), (Object[]) rs.getArray(2).getArray());
                    assertArrayEquals(array3.toArray(), (Object[]) rs.getArray(4).getArray());
                }
            });
        });

    }

    @Test
    public void successfullyBatchInsertMap() throws Exception {
        withStatement(statement -> {
            statement.execute("SET allow_experimental_map_type=1;");
            statement.execute("DROP TABLE IF EXISTS test");
            statement.execute("CREATE TABLE test(a Map(Int32, Int32), b Map(String, String))ENGINE=Log");

            withPreparedStatement(statement.getConnection(), "INSERT INTO test VALUES(?, ?)", pstmt -> {
                Map<Integer, Integer> row1col1 = new HashMap<>();
                Map<String, String> row1col2 = new HashMap<>();
                row1col1.put(1, 1);
                row1col1.put(2, 2);
                row1col2.put("a", "b");
                pstmt.setObject(1, row1col1);
                pstmt.setObject(2, row1col2);
                pstmt.addBatch();

                Map<Integer, Integer> row2col1 = new HashMap<>();
                Map<String, String> row2col2 = new HashMap<>();
                row2col1.put(3, 3);
                row2col2.put("b", "c");
                pstmt.setObject(1, row2col1);
                pstmt.setObject(2, row2col2);
                pstmt.addBatch();

                assertBatchInsertResult(pstmt.executeBatch(), 2);

                ResultSet rs = statement.executeQuery("SELECT * FROM test");

                assertTrue(rs.next());
                Map<Integer, Integer> map1 = (Map<Integer, Integer>) rs.getObject(1);
                assertEquals(1, map1.get(1));
                assertEquals(2, map1.get(2));
                Map<String, String> map2 = (Map<String, String>) rs.getObject(2);
                assertEquals("b", map2.get("a"));

                assertTrue(rs.next());
                Map<Integer, Integer> map3 = (Map<Integer, Integer>) rs.getObject(1);
                assertEquals(3, map3.get(3));
                Map<String, String> map4 = (Map<String, String>) rs.getObject(2);
                assertEquals("c", map4.get("b"));

                assertFalse(rs.next());
            });
        });

    }

    @Test
    public void successfullyBatchInsertDateTime() throws Exception {
        withStatement(statement -> {
            // 2018-07-01 00:00:00 Asia/Shanghai
            long time = 1530374400;

            statement.execute("DROP TABLE IF EXISTS test");
            statement.execute("CREATE TABLE test(time DateTime)ENGINE=Log");

            withPreparedStatement(statement.getConnection(), "INSERT INTO test VALUES(?)", pstmt -> {
                long insertTime = time;
                for (int i = 0; i < 24; i++) {
                    pstmt.setTimestamp(1, new Timestamp(insertTime * 1000));
                    pstmt.addBatch();
                    insertTime += 3600;
                }
                assertBatchInsertResult(pstmt.executeBatch(), 24);
            });

            long selectTime = time;
            ResultSet rs = statement.executeQuery("SELECT * FROM test ORDER BY time ASC");
            while (rs.next()) {
                assertEquals(selectTime * 1000, rs.getTimestamp(1).getTime());
                selectTime += 3600;
            }
        });

    }

    @Test
    public void successfullyBatchInsertLowCardinality() throws Exception {
        withStatement(statement -> {
            statement.execute("DROP TABLE IF EXISTS test");
            statement.execute("CREATE TABLE test(i LowCardinality(String)) ENGINE=Log");

            withPreparedStatement(statement.getConnection(), "INSERT INTO test VALUES(?)", pstmt -> {
                pstmt.setObject(1, "test");
                pstmt.addBatch();

                assertBatchInsertResult(pstmt.executeBatch(), 1);
            });
            ResultSet rs = statement.executeQuery("SELECT * FROM test");

            assertTrue(rs.next());
            assertEquals("test", rs.getObject(1));

        });

    }

    @Test
    public void batchInsert_withoutAllParameters_throwSqlException() throws Exception {
        withStatement(statement -> {
            statement.execute("DROP TABLE IF EXISTS test");
            statement.execute("CREATE TABLE test(id Int8, age UInt8, name String, name2 String)ENGINE=Log");

            withPreparedStatement(statement.getConnection(), "INSERT INTO test VALUES(?, 1, ?, ?)", pstmt -> {
                pstmt.setByte(1, (byte) 1);
                pstmt.setString(2, "Zhang San" + 1);
                assertThrows(SQLException.class, () -> pstmt.addBatch());
            });
        });

    }
}
