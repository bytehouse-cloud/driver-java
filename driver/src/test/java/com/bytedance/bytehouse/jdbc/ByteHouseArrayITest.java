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

import com.bytedance.bytehouse.data.type.DataTypeFloat32;
import com.bytedance.bytehouse.data.type.DataTypeFloat64;
import com.bytedance.bytehouse.data.type.DataTypeInt16;
import com.bytedance.bytehouse.data.type.DataTypeInt32;
import com.bytedance.bytehouse.data.type.DataTypeInt64;
import com.bytedance.bytehouse.data.type.DataTypeInt8;
import com.bytedance.bytehouse.data.type.DataTypeUInt16;
import com.bytedance.bytehouse.data.type.DataTypeUInt32;
import com.bytedance.bytehouse.data.type.DataTypeUInt64;
import com.bytedance.bytehouse.data.type.DataTypeUInt8;
import org.junit.jupiter.api.Test;
import java.math.BigInteger;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ByteHouseArrayITest extends AbstractITest {
    private static final Short UINT8_MIN_VALUE = 0;
    private static final Short UINT8_MAX_VALUE = 255;
    private static final Integer UINT16_MIN_VALUE = 0;
    private static final Integer UINT16_MAX_VALUE = 65535;
    private static final Long UINT32_MIN_VALUE = 0L;
    private static final Long UINT32_MAX_VALUE = 4294967295L;
    private static final BigInteger UINT64_MIN_VALUE = BigInteger.valueOf(0L);
    private static final BigInteger UINT64_MAX_VALUE = new BigInteger("18446744073709551615");

    private static final Byte INT8_MIN_VALUE = -128;
    private static final Byte INT8_MAX_VALUE = 127;
    private static final Short INT16_MIN_VALUE = -32768;
    private static final Short INT16_MAX_VALUE = 32767;
    private static final Integer INT32_MIN_VALUE = -2147483648;
    private static final Integer INT32_MAX_VALUE = 2147483647;
    private static final Long INT64_MIN_VALUE = -9223372036854775808L;
    private static final Long INT64_MAX_VALUE = 9223372036854775807L;

    public void assertArrayEquals(Object[] expected, Object[] actual) {
        assertEquals(expected.length, actual.length);
        for (int i=0; i<expected.length; i++) {
            assertEquals(expected[i], actual[i]);
        }
    }

    @Test
    public void testUIntArray() throws Exception {
        withStatement(statement -> {
            String databaseName = getDatabaseName();
            String tableName = databaseName + "." + getTableName();


            try {
                statement.execute(String.format("CREATE DATABASE %s", databaseName));
                statement.execute(String.format("CREATE TABLE %s(ua8 Array(UInt8), ua16 Array(UInt16), ua32 Array(UInt32), ua64 Array(UInt64))"
                        + " ENGINE=CnchMergeTree() order by tuple()", tableName));

                String insertSql = String.format("INSERT INTO %s VALUES (?, ?, ?, ?)", tableName);
                PreparedStatement preparedStatement = statement.getConnection().prepareStatement(insertSql);

                Short[] ua8Array = new Short[]{UINT8_MIN_VALUE, UINT8_MAX_VALUE};
                Integer[] ua16Array = new Integer[]{UINT16_MIN_VALUE, UINT16_MAX_VALUE};
                Long[] ua32Array = new Long[]{UINT32_MIN_VALUE, UINT32_MAX_VALUE};
                BigInteger[] ua64Array = new BigInteger[]{UINT64_MIN_VALUE, UINT64_MAX_VALUE};

                preparedStatement.setArray(1, new ByteHouseArray(new DataTypeUInt8(), new Short[]{UINT8_MIN_VALUE, UINT8_MAX_VALUE}));
                preparedStatement.setArray(2, new ByteHouseArray(new DataTypeUInt16(), new Integer[]{UINT16_MIN_VALUE, UINT16_MAX_VALUE}));
                preparedStatement.setArray(3, new ByteHouseArray(new DataTypeUInt32(), new Long[]{UINT32_MIN_VALUE, UINT32_MAX_VALUE}));
                preparedStatement.setArray(4, new ByteHouseArray(new DataTypeUInt64(), new BigInteger[]{UINT64_MIN_VALUE, UINT64_MAX_VALUE}));
                preparedStatement.addBatch();
                preparedStatement.executeBatch();

                ResultSet rs = statement.executeQuery(String.format("SELECT * FROM %s", tableName));
                while (rs.next()) {
                    Short[] ua8ArrayResult = (Short[]) rs.getArray(1).getArray();
                    Integer[] ua16ArrayResult = (Integer[]) rs.getArray(2).getArray();
                    Long[] ua32ArrayResult = (Long[]) rs.getArray(3).getArray();
                    BigInteger[] ua64ArrayResult = (BigInteger[]) rs.getArray(4).getArray();
                    assertArrayEquals(ua8Array, ua8ArrayResult);
                    assertArrayEquals(ua16Array, ua16ArrayResult);
                    assertArrayEquals(ua32Array, ua32ArrayResult);
                    assertArrayEquals(ua64Array, ua64ArrayResult);
                }
            }
            finally {
                statement.execute(String.format("DROP DATABASE %s", databaseName));
            }
        });
    }

    @Test
    public void testIntArray() throws Exception {
        withStatement(statement -> {
            String databaseName = getDatabaseName();
            String tableName = databaseName + "." + getTableName();


            try {
                statement.execute(String.format("CREATE DATABASE %s", databaseName));
                statement.execute(String.format("CREATE TABLE %s(a8 Array(Int8), a16 Array(Int16), a32 Array(Int32), a64 Array(Int64))"
                        + " ENGINE=CnchMergeTree() order by tuple()", tableName));

                String insertSql = String.format("INSERT INTO %s VALUES (?, ?, ?, ?)", tableName);
                PreparedStatement preparedStatement = statement.getConnection().prepareStatement(insertSql);

                Byte[] a8Array = new Byte[]{INT8_MIN_VALUE, INT8_MAX_VALUE};
                Short[] a16Array = new Short[]{INT16_MIN_VALUE, INT16_MAX_VALUE};
                Integer[] a32Array = new Integer[]{INT32_MIN_VALUE, INT32_MAX_VALUE};
                Long[] a64Array = new Long[]{INT64_MIN_VALUE, INT64_MAX_VALUE};

                preparedStatement.setArray(1, new ByteHouseArray(new DataTypeInt8(), new Byte[]{INT8_MIN_VALUE, INT8_MAX_VALUE}));
                preparedStatement.setArray(2, new ByteHouseArray(new DataTypeInt16(), new Short[]{INT16_MIN_VALUE, INT16_MAX_VALUE}));
                preparedStatement.setArray(3, new ByteHouseArray(new DataTypeInt32(), new Integer[]{INT32_MIN_VALUE, INT32_MAX_VALUE}));
                preparedStatement.setArray(4, new ByteHouseArray(new DataTypeInt64(), new Long[]{INT64_MIN_VALUE, INT64_MAX_VALUE}));
                preparedStatement.addBatch();
                preparedStatement.executeBatch();

                ResultSet rs = statement.executeQuery(String.format("SELECT * FROM %s", tableName));
                while (rs.next()) {
                    Byte[] a8ArrayResult = (Byte[]) rs.getArray(1).getArray();
                    Short[] a16ArrayResult = (Short[]) rs.getArray(2).getArray();
                    Integer[] a32ArrayResult = (Integer[]) rs.getArray(3).getArray();
                    Long[] a64ArrayResult = (Long[]) rs.getArray(4).getArray();
                    assertArrayEquals(a8Array, a8ArrayResult);
                    assertArrayEquals(a16Array, a16ArrayResult);
                    assertArrayEquals(a32Array, a32ArrayResult);
                    assertArrayEquals(a64Array, a64ArrayResult);
                }
            }
            finally {
                statement.execute(String.format("DROP DATABASE %s", databaseName));
            }
        });
    }

    @Test
    public void testFloatArray() throws Exception {
        withStatement(statement -> {
            String databaseName = getDatabaseName();
            String tableName = databaseName + "." + getTableName();


            try {
                statement.execute(String.format("CREATE DATABASE %s", databaseName));
                statement.execute(String.format("CREATE TABLE %s(f32 Array(Float32), f64 Array(Float64))"
                        + " ENGINE=CnchMergeTree() order by tuple()", tableName));

                String insertSql = String.format("INSERT INTO %s VALUES (?, ?)", tableName);
                PreparedStatement preparedStatement = statement.getConnection().prepareStatement(insertSql);

                Float[] f32Array = new Float[]{Float.MIN_VALUE, Float.MAX_VALUE};
                Double[] f64Array = new Double[]{Double.MIN_VALUE, Double.MAX_VALUE};

                preparedStatement.setArray(1, new ByteHouseArray(new DataTypeFloat32(), new Float[]{Float.MIN_VALUE, Float.MAX_VALUE}));
                preparedStatement.setArray(2, new ByteHouseArray(new DataTypeFloat64(), new Double[]{Double.MIN_VALUE, Double.MAX_VALUE}));
                preparedStatement.addBatch();
                preparedStatement.executeBatch();

                ResultSet rs = statement.executeQuery(String.format("SELECT * FROM %s", tableName));
                while (rs.next()) {
                    Float[] f32ArrayResult = (Float[]) rs.getArray(1).getArray();
                    Double[] f64ArrayResult = (Double[]) rs.getArray(2).getArray();
                    assertArrayEquals(f32Array, f32ArrayResult);
                    assertArrayEquals(f64Array, f64ArrayResult);
                }
            }
            finally {
                statement.execute(String.format("DROP DATABASE %s", databaseName));
            }
        });
    }

    @Test
    public void testSetArrayPrimitive() throws Exception {
        withStatement(statement -> {
            String databaseName = getDatabaseName();
            String tableName = databaseName + "." + getTableName();


            try {
                statement.execute(String.format("CREATE DATABASE %s", databaseName));
                statement.execute(String.format("CREATE TABLE %s(a8 Array(Int8), a16 Array(Int16), a32 Array(Int32), a64 Array(Int64))"
                        + " ENGINE=CnchMergeTree() order by tuple()", tableName));

                String insertSql = String.format("INSERT INTO %s VALUES (?, ?, ?, ?)", tableName);
                PreparedStatement preparedStatement = statement.getConnection().prepareStatement(insertSql);

                Byte[] a8Array = new Byte[]{INT8_MIN_VALUE, INT8_MAX_VALUE};
                Short[] a16Array = new Short[]{INT16_MIN_VALUE, INT16_MAX_VALUE};
                Integer[] a32Array = new Integer[]{INT32_MIN_VALUE, INT32_MAX_VALUE};
                Long[] a64Array = new Long[]{INT64_MIN_VALUE, INT64_MAX_VALUE};

                preparedStatement.setArray(1, new ByteHouseArray(new DataTypeInt8(), new byte[]{INT8_MIN_VALUE, INT8_MAX_VALUE}));
                preparedStatement.setArray(2, new ByteHouseArray(new DataTypeInt16(), new short[]{INT16_MIN_VALUE, INT16_MAX_VALUE}));
                preparedStatement.setArray(3, new ByteHouseArray(new DataTypeInt32(), new int[]{INT32_MIN_VALUE, INT32_MAX_VALUE}));
                preparedStatement.setArray(4, new ByteHouseArray(new DataTypeInt64(), new long[]{INT64_MIN_VALUE, INT64_MAX_VALUE}));
                preparedStatement.addBatch();
                preparedStatement.executeBatch();

                ResultSet rs = statement.executeQuery(String.format("SELECT * FROM %s", tableName));
                while (rs.next()) {
                    Byte[] a8ArrayResult = (Byte[]) rs.getArray(1).getArray();
                    Short[] a16ArrayResult = (Short[]) rs.getArray(2).getArray();
                    Integer[] a32ArrayResult = (Integer[]) rs.getArray(3).getArray();
                    Long[] a64ArrayResult = (Long[]) rs.getArray(4).getArray();
                    assertArrayEquals(a8Array, a8ArrayResult);
                    assertArrayEquals(a16Array, a16ArrayResult);
                    assertArrayEquals(a32Array, a32ArrayResult);
                    assertArrayEquals(a64Array, a64ArrayResult);
                }
            }
            finally {
                statement.execute(String.format("DROP DATABASE %s", databaseName));
            }
        });
    }
}
