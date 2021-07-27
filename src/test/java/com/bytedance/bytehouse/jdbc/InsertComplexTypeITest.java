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

import java.sql.ResultSet;
import java.sql.Struct;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class InsertComplexTypeITest extends AbstractITest {

    @Test
    public void successfullyArrayDataType() throws Exception {
        withStatement(statement -> {
            statement.executeQuery("DROP TABLE IF EXISTS test");
            statement.executeQuery("CREATE TABLE test(test_Array Array(UInt8), test_Array2 Array(Array(String)), n3 Array(Nullable(UInt8)) )ENGINE=Log");
            statement.executeQuery("INSERT INTO test VALUES ([1, 2, 3, 4], [ ['1', '2'] ], [1, 2, NULL] )");
            ResultSet rs = statement.executeQuery("SELECT * FROM test");
            assertTrue(rs.next());
            assertArrayEquals(new Short[]{1, 2, 3, 4}, (Object[]) rs.getArray(1).getArray());
            Object[] objects = (Object[]) rs.getArray(2).getArray();
            ByteHouseArray array = (ByteHouseArray) objects[0];
            assertArrayEquals(new Object[]{"1", "2"}, array.getArray());

            objects = (Object[]) rs.getArray(3).getArray();
            assertArrayEquals(new Short[]{1, 2, null}, objects);

            assertFalse(rs.next());
            statement.executeQuery("DROP TABLE IF EXISTS test");
        });
    }

    @Test
    public void successfullyFixedStringDataType() throws Exception {
        withStatement(statement -> {
            statement.executeQuery("DROP TABLE IF EXISTS test");
            statement.executeQuery("CREATE TABLE test(str FixedString(3))ENGINE=Log");
            statement.executeQuery("INSERT INTO test VALUES('abc')");

            withPreparedStatement(statement.getConnection(), "INSERT INTO test VALUES(?)", pstmt -> {
                pstmt.setObject(1, "abc");
                pstmt.executeUpdate();
            });

            ResultSet rs = statement.executeQuery("SELECT str, COUNT(0) FROM test group by str");
            assertTrue(rs.next());
            assertEquals("abc", rs.getString(1));
            assertEquals(2, rs.getInt(2));
            assertFalse(rs.next());
            statement.executeQuery("DROP TABLE IF EXISTS test");
        });
    }

    @Test
    public void successfullyNullableDataType() throws Exception {
        withStatement(statement -> {
            statement.executeQuery("DROP TABLE IF EXISTS test");
            statement.executeQuery("CREATE TABLE test(test_nullable Nullable(UInt8))ENGINE=Log");
            statement.executeQuery("INSERT INTO test VALUES(Null)(1)(3)(Null)");
            ResultSet rs = statement.executeQuery("SELECT * FROM test ORDER BY test_nullable");
            assertTrue(rs.next());
            assertEquals(1, rs.getByte(1));
            assertTrue(rs.next());
            assertEquals(3, rs.getByte(1));
            assertTrue(rs.next());
            assertEquals(0, rs.getByte(1));
            assertTrue(rs.wasNull());
            assertEquals(0, rs.getByte(1));
            assertTrue(rs.wasNull());
            statement.executeQuery("DROP TABLE IF EXISTS test");
        });
    }

    @Test
    public void successfullyDateTimeDataType() throws Exception {
        withStatement(statement -> {
            statement.executeQuery("DROP TABLE IF EXISTS test");
            statement.executeQuery("CREATE TABLE test(test_datetime DateTime('UTC'), test_datetime2 DateTime('Asia/Shanghai') )ENGINE=Log");
            statement.executeQuery("INSERT INTO test VALUES('2000-01-01 08:01:01', '2000-01-01 08:01:01')");
            ResultSet rs = statement.executeQuery("SELECT * FROM test");
            assertTrue(rs.next());

            assertEquals(
                Timestamp.valueOf(LocalDateTime.of(2000, 1, 1, 8, 1, 1, 0)).getTime(),
                rs.getTimestamp(1).getTime());

            assertEquals(
                Timestamp.valueOf(LocalDateTime.of(2000, 1, 1, 8, 1, 1, 0)).getTime(),
                rs.getTimestamp(2).getTime());

            assertFalse(rs.next());
            statement.executeQuery("DROP TABLE IF EXISTS test");
        }, "use_client_time_zone", true);
    }

    @Test
    public void successfullyDateTime64DataType() throws Exception {
        withStatement(statement -> {
            statement.executeQuery("DROP TABLE IF EXISTS test");
            statement.executeQuery("CREATE TABLE test(seq UInt8, test_datetime DateTime64(9, 'UTC'))ENGINE=Log");
            statement.executeQuery("INSERT INTO test VALUES(1, toDateTime64('2000-01-01 00:01:01.123456789'))");
            statement.executeQuery("INSERT INTO test VALUES(2, toDateTime64('2000-01-01 00:01:01.0234567'))");
            statement.executeQuery("INSERT INTO test VALUES(3, toDateTime64('2000-01-01 00:01:01.0234567889'))");
            ResultSet rs = statement.executeQuery("SELECT * FROM test ORDER BY seq");
            assertTrue(rs.next());
            assertEquals(Timestamp.valueOf(LocalDateTime.of(2000, 1, 1, 0, 1, 1, 123456789)), rs.getTimestamp(2));
            assertTrue(rs.next());
            assertEquals(Timestamp.valueOf(LocalDateTime.of(2000, 1, 1, 0, 1, 1, 23456700)), rs.getTimestamp(2));
            assertTrue(rs.next());
            assertEquals(Timestamp.valueOf(LocalDateTime.of(2000, 1, 1, 0, 1, 1, 23456789)), rs.getTimestamp(2));
            assertFalse(rs.next());
            statement.executeQuery("DROP TABLE IF EXISTS test");
        }, "use_client_time_zone", true);
    }

    @Test
    public void successfullyMinDateTime64DataType() throws Exception {
        withStatement(statement -> {
            statement.executeQuery("DROP TABLE IF EXISTS test");
            statement.executeQuery("CREATE TABLE test(test_datetime DateTime64(9, 'UTC'))ENGINE=Log");
            statement.executeQuery("INSERT INTO test VALUES(toDateTime64('1970-01-01 00:00:00.000000000'))");
            ResultSet rs = statement.executeQuery("SELECT * FROM test");
            assertTrue(rs.next());
            assertEquals(
                Timestamp.valueOf(LocalDateTime.of(1970, 1, 1, 0, 0, 0, 0)),
                rs.getTimestamp(1));
            assertFalse(rs.next());
            statement.executeQuery("DROP TABLE IF EXISTS test");
        }, "use_client_time_zone", true);
    }

    @Test
    public void successfullyMaxDateTime64DataType() throws Exception {
        withStatement(statement -> {
            statement.executeQuery("DROP TABLE IF EXISTS test");
            statement.executeQuery("CREATE TABLE test(test_datetime DateTime64(9, 'UTC'))ENGINE=Log");
            statement.executeQuery("INSERT INTO test VALUES(toDateTime64('2105-12-31 23:59:59.999999999'))");
            ResultSet rs = statement.executeQuery("SELECT * FROM test");
            assertTrue(rs.next());

            assertEquals(
                Timestamp.valueOf(LocalDateTime.of(2105, 12, 31, 23, 59, 59, 999999999)),
                rs.getTimestamp(1));
            assertFalse(rs.next());
            statement.executeQuery("DROP TABLE IF EXISTS test");
        }, "use_client_time_zone", true);
    }

    @Test
    public void successfullyTupleDataType() throws Exception {
        withStatement(statement -> {
            statement.executeQuery("DROP TABLE IF EXISTS test");
            statement.executeQuery("CREATE TABLE test(test_tuple Tuple(String, UInt8),"
                                   + " tuple_array  Tuple(Array(Nullable(String)), Nullable(UInt8)),"
                                   + " array_tuple Array(Tuple(UInt32, Nullable(String)) )"
                                   + " )ENGINE=Log");
            statement.executeQuery("INSERT INTO test VALUES( ('test_string', 1), (['1'], 32), [(32, '1'), (22, NULL) ] )");
            ResultSet rs = statement.executeQuery("SELECT * FROM test");
            assertTrue(rs.next());
            assertArrayEquals(
                new Object[]{"test_string", (short) (1)},
                ((Struct) rs.getObject(1)).getAttributes());

            Object []objs = ((Struct) rs.getObject(2)).getAttributes();

            ByteHouseArray arr = (ByteHouseArray) (objs[0]);
            assertArrayEquals(new Object[]{"1"}, arr.getArray());
            assertEquals((short) 32, objs[1]);

            arr = (ByteHouseArray) rs.getObject(3);

            ByteHouseStruct t1 = (ByteHouseStruct) ((Object[]) arr.getArray())[0];
            ByteHouseStruct t2 = (ByteHouseStruct) ((Object[]) arr.getArray())[1];
            assertArrayEquals(new Object[]{(long) 32, "1"}, t1.getAttributes());
            assertArrayEquals(new Object[]{(long) 22, null}, t2.getAttributes());

            assertFalse(rs.next());
            statement.executeQuery("DROP TABLE IF EXISTS test");
        });
    }

    @Test
    public void successfullyMapDataType() throws Exception {
        withStatement(statement -> {
            statement.execute("SET allow_experimental_map_type=1;");
            statement.execute("DROP TABLE IF EXISTS test");
            statement.execute("CREATE TABLE test(a Map(UInt32, UInt32), b Map(String, String))ENGINE=Log");

            statement.executeUpdate(
                    "INSERT INTO test VALUES ({1 : 1, 2 : 2}, {'a': 'b'}), ({},{})"
            );


            ResultSet rs = statement.executeQuery("SELECT * FROM test");

            assertTrue(rs.next());
            Map<Long, Long> map1 = (Map<Long, Long>) rs.getObject(1);
            assertEquals(1L, map1.get(1L));
            assertEquals(2L, map1.get(2L));
            Map<String, String> map2 = (Map<String, String>) rs.getObject(2);
            assertEquals("b", map2.get("a"));

            assertTrue(rs.next());
            Map<Integer, Integer> map3 = (Map<Integer, Integer>) rs.getObject(1);
            assertEquals(0, map3.size());
            Map<String, String> map4 = (Map<String, String>) rs.getObject(2);
            assertEquals(0, map4.size());

            assertFalse(rs.next());

            statement.execute("DROP TABLE IF EXISTS test");
        });
    }

    @Test
    public void successfullyMapDataTypeNested() throws Exception {
        withStatement(statement -> {
            statement.execute("SET allow_experimental_map_type=1;");
            statement.execute("DROP TABLE IF EXISTS test");
            statement.execute(
                    "CREATE TABLE test(a Map(Int32, Array(Int32)), b Map(String, Array(Array(String))))ENGINE=Log"
            );

            statement.executeUpdate(
                    "INSERT INTO test VALUES ({1: [1, 2, 3]}, {'a': [['b', 'c'], ['d']]})"
            );
            ResultSet rs = statement.executeQuery("SELECT * FROM test");

            assertTrue(rs.next());
            Map<Integer, ByteHouseArray> map1 = (Map<Integer, ByteHouseArray>) rs.getObject(1);
            assertArrayEquals(new Object[]{1, 2, 3}, map1.get(1).getArray());

            Map<String, ByteHouseArray> map2 = (Map<String, ByteHouseArray>) rs.getObject(2);
            ByteHouseArray map2arr1 = ((ByteHouseArray) map2.get("a").getArray()[0]);
            assertArrayEquals(new Object[]{"b", "c"}, map2arr1.getArray());
            ByteHouseArray map2arr2 = ((ByteHouseArray) map2.get("a").getArray()[1]);
            assertArrayEquals(new Object[]{"d"}, map2arr2.getArray());

            assertFalse(rs.next());

            statement.execute("DROP TABLE IF EXISTS test");
        });
    }
}
