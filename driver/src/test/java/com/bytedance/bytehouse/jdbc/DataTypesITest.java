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

import com.bytedance.bytehouse.data.type.DataTypeInt32;
import org.junit.jupiter.api.Test;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Inet6Address;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class DataTypesITest extends AbstractITest {
    @Test
    public void testDataTypeUInt8() throws Exception {
        withStatement(statement -> {
            String databaseName = getDatabaseName();
            String tableName = databaseName + "." + getTableName();

            try {
                statement.execute(String.format("CREATE DATABASE %s", databaseName));
                statement.execute(String.format("CREATE TABLE %s(id UInt8)"
                        + " ENGINE=CnchMergeTree() order by tuple()", tableName));

                withPreparedStatement(String.format("INSERT INTO %s VALUES(?)", tableName), pstmt -> {
                    Short valueShort = 1;
                    short valueShortPrimitive = 1;

                    pstmt.setShort(1, valueShort);
                    pstmt.addBatch();

                    pstmt.setShort(1, valueShortPrimitive);
                    pstmt.addBatch();

                    pstmt.setObject(1, valueShort);
                    pstmt.addBatch();

                    pstmt.setObject(1, valueShortPrimitive);
                    pstmt.addBatch();

                    pstmt.executeBatch();
                });
            }
            finally {
                statement.execute(String.format("DROP DATABASE %s", databaseName));
            }
        });
    }

    @Test
    public void testDataTypeUInt16() throws Exception {
        withStatement(statement -> {
            String databaseName = getDatabaseName();
            String tableName = databaseName + "." + getTableName();

            try {
                statement.execute(String.format("CREATE DATABASE %s", databaseName));
                statement.execute(String.format("CREATE TABLE %s(id UInt16)"
                        + " ENGINE=CnchMergeTree() order by tuple()", tableName));

                withPreparedStatement(String.format("INSERT INTO %s VALUES(?)", tableName), pstmt -> {
                    Integer valueInteger = 35000;
                    int valueIntegerPrimitive = 35000;

                    pstmt.setInt(1, valueInteger);
                    pstmt.addBatch();

                    pstmt.setInt(1, valueIntegerPrimitive);
                    pstmt.addBatch();

                    pstmt.setObject(1, valueInteger);
                    pstmt.addBatch();

                    pstmt.setObject(1, valueIntegerPrimitive);
                    pstmt.addBatch();

                    pstmt.executeBatch();
                });
            }
            finally {
                statement.execute(String.format("DROP DATABASE %s", databaseName));
            }
        });
    }

    @Test
    public void testDataTypeUInt32() throws Exception {
        withStatement(statement -> {
            String databaseName = getDatabaseName();
            String tableName = databaseName + "." + getTableName();

            try {
                statement.execute(String.format("CREATE DATABASE %s", databaseName));
                statement.execute(String.format("CREATE TABLE %s(id UInt32)"
                        + " ENGINE=CnchMergeTree() order by tuple()", tableName));

                withPreparedStatement(String.format("INSERT INTO %s VALUES(?)", tableName), pstmt -> {
                    Long valueLong = Long.valueOf(256256256256L);
                    long valueLongPrimitive = 256256256256L;

                    pstmt.setLong(1, valueLong);
                    pstmt.addBatch();

                    pstmt.setLong(1, valueLongPrimitive);
                    pstmt.addBatch();

                    pstmt.setObject(1, valueLong);
                    pstmt.addBatch();

                    pstmt.setObject(1, valueLongPrimitive);
                    pstmt.addBatch();

                    pstmt.executeBatch();
                });
            }
            finally {
                statement.execute(String.format("DROP DATABASE %s", databaseName));
            }
        });
    }

    @Test
    public void testDataTypeUInt64() throws Exception {
        withStatement(statement -> {
            String databaseName = getDatabaseName();
            String tableName = databaseName + "." + getTableName();

            try {
                statement.execute(String.format("CREATE DATABASE %s", databaseName));
                statement.execute(String.format("CREATE TABLE %s(id UInt64)"
                        + " ENGINE=CnchMergeTree() order by tuple()", tableName));

                withPreparedStatement(String.format("INSERT INTO %s VALUES(?)", tableName), pstmt -> {
                    BigInteger bigIntegerValue = BigInteger.valueOf(256256256256L);

                    pstmt.setObject(1, bigIntegerValue);
                    pstmt.addBatch();

                    pstmt.executeBatch();
                });
            }
            finally {
                statement.execute(String.format("DROP DATABASE %s", databaseName));
            }
        });
    }

    @Test
    public void testDataTypeInt8() throws Exception {
        withStatement(statement -> {
            String databaseName = getDatabaseName();
            String tableName = databaseName + "." + getTableName();

            try {
                statement.execute(String.format("CREATE DATABASE %s", databaseName));
                statement.execute(String.format("CREATE TABLE %s(id Int8)"
                        + " ENGINE=CnchMergeTree() order by tuple()", tableName));

                withPreparedStatement(String.format("INSERT INTO %s VALUES(?)", tableName), pstmt -> {
                    Byte valueByte = 1;
                    byte valueBytePrimitive = 1;

                    pstmt.setByte(1, valueByte);
                    pstmt.addBatch();

                    pstmt.setByte(1, valueBytePrimitive);
                    pstmt.addBatch();

                    pstmt.setObject(1, valueByte);
                    pstmt.addBatch();

                    pstmt.setObject(1, valueBytePrimitive);
                    pstmt.addBatch();

                    pstmt.executeBatch();
                });
            }
            finally {
                statement.execute(String.format("DROP DATABASE %s", databaseName));
            }
        });
    }

    @Test
    public void testDataTypeInt16() throws Exception {
        withStatement(statement -> {
            String databaseName = getDatabaseName();
            String tableName = databaseName + "." + getTableName();

            try {
                statement.execute(String.format("CREATE DATABASE %s", databaseName));
                statement.execute(String.format("CREATE TABLE %s(id Int16)"
                        + " ENGINE=CnchMergeTree() order by tuple()", tableName));

                withPreparedStatement(String.format("INSERT INTO %s VALUES(?)", tableName), pstmt -> {
                    Short valueShort = 1;
                    short valueShortPrimitive = 1;

                    pstmt.setShort(1, valueShort);
                    pstmt.addBatch();

                    pstmt.setShort(1, valueShortPrimitive);
                    pstmt.addBatch();

                    pstmt.setObject(1, valueShort);
                    pstmt.addBatch();

                    pstmt.setObject(1, valueShortPrimitive);
                    pstmt.addBatch();

                    pstmt.executeBatch();
                });
            }
            finally {
                statement.execute(String.format("DROP DATABASE %s", databaseName));
            }
        });
    }

    @Test
    public void testDataTypeInt32() throws Exception {
        withStatement(statement -> {
            String databaseName = getDatabaseName();
            String tableName = databaseName + "." + getTableName();

            try {
                statement.execute(String.format("CREATE DATABASE %s", databaseName));
                statement.execute(String.format("CREATE TABLE %s(id Int32)"
                        + " ENGINE=CnchMergeTree() order by tuple()", tableName));

                withPreparedStatement(String.format("INSERT INTO %s VALUES(?)", tableName), pstmt -> {
                    Integer valueInteger = 35000;
                    int valueIntegerPrimitive = 35000;

                    pstmt.setInt(1, valueInteger);
                    pstmt.addBatch();

                    pstmt.setInt(1, valueIntegerPrimitive);
                    pstmt.addBatch();

                    pstmt.setObject(1, valueInteger);
                    pstmt.addBatch();

                    pstmt.setObject(1, valueIntegerPrimitive);
                    pstmt.addBatch();

                    pstmt.executeBatch();
                });
            }
            finally {
                statement.execute(String.format("DROP DATABASE %s", databaseName));
            }
        });
    }

    @Test
    public void testDataTypeInt64() throws Exception {
        withStatement(statement -> {
            String databaseName = getDatabaseName();
            String tableName = databaseName + "." + getTableName();

            try {
                statement.execute(String.format("CREATE DATABASE %s", databaseName));
                statement.execute(String.format("CREATE TABLE %s(id Int64)"
                        + " ENGINE=CnchMergeTree() order by tuple()", tableName));

                withPreparedStatement(String.format("INSERT INTO %s VALUES(?)", tableName), pstmt -> {
                    Long valueLong = Long.valueOf(256256256256L);
                    long valueLongPrimitive = 256256256256L;

                    pstmt.setLong(1, valueLong);
                    pstmt.addBatch();

                    pstmt.setLong(1, valueLongPrimitive);
                    pstmt.addBatch();

                    pstmt.setObject(1, valueLong);
                    pstmt.addBatch();

                    pstmt.setObject(1, valueLongPrimitive);
                    pstmt.addBatch();

                    pstmt.executeBatch();
                });
            }
            finally {
                statement.execute(String.format("DROP DATABASE %s", databaseName));
            }
        });
    }

    @Test
    public void testDataTypeFloat32() throws Exception {
        withStatement(statement -> {
            String databaseName = getDatabaseName();
            String tableName = databaseName + "." + getTableName();

            try {
                statement.execute(String.format("CREATE DATABASE %s", databaseName));
                statement.execute(String.format("CREATE TABLE %s(id Float32)"
                        + " ENGINE=CnchMergeTree() order by tuple()", tableName));

                withPreparedStatement(String.format("INSERT INTO %s VALUES(?)", tableName), pstmt -> {
                    Float valueFloat = Float.valueOf("123.45879");
                    float valueFloatPrimitive = 123.45897f;

                    pstmt.setFloat(1, valueFloat);
                    pstmt.addBatch();

                    pstmt.setFloat(1, valueFloatPrimitive);
                    pstmt.addBatch();

                    pstmt.setObject(1, valueFloat);
                    pstmt.addBatch();

                    pstmt.setObject(1, valueFloatPrimitive);
                    pstmt.addBatch();

                    pstmt.executeBatch();
                });
            }
            finally {
                statement.execute(String.format("DROP DATABASE %s", databaseName));
            }
        });
    }

    @Test
    public void testDataTypeFloat64() throws Exception {
        withStatement(statement -> {
            String databaseName = getDatabaseName();
            String tableName = databaseName + "." + getTableName();

            try {
                statement.execute(String.format("CREATE DATABASE %s", databaseName));
                statement.execute(String.format("CREATE TABLE %s(id Float64)"
                        + " ENGINE=CnchMergeTree() order by tuple()", tableName));

                withPreparedStatement(String.format("INSERT INTO %s VALUES(?)", tableName), pstmt -> {
                    Double valueDouble = Double.valueOf("123.45879");
                    double valueDoublePrimitive = 123.45897;

                    pstmt.setDouble(1, valueDouble);
                    pstmt.addBatch();

                    pstmt.setDouble(1, valueDoublePrimitive);
                    pstmt.addBatch();

                    pstmt.setObject(1, valueDouble);
                    pstmt.addBatch();

                    pstmt.setObject(1, valueDoublePrimitive);
                    pstmt.addBatch();

                    pstmt.executeBatch();
                });
            }
            finally {
                statement.execute(String.format("DROP DATABASE %s", databaseName));
            }
        });
    }

    @Test
    public void testDataTypeDecimal() throws Exception {
        withStatement(statement -> {
            String databaseName = getDatabaseName();
            String tableName = databaseName + "." + getTableName();

            try {
                statement.execute(String.format("CREATE DATABASE %s", databaseName));
                statement.execute(String.format("CREATE TABLE %s(id Decimal(38,18))"
                        + " ENGINE=CnchMergeTree() order by tuple()", tableName));

                withPreparedStatement(String.format("INSERT INTO %s VALUES(?)", tableName), pstmt -> {
                    BigDecimal valueBigDecimal = BigDecimal.valueOf(123.456);

                    pstmt.setBigDecimal(1, valueBigDecimal);
                    pstmt.addBatch();

                    pstmt.setObject(1, valueBigDecimal);
                    pstmt.addBatch();

                    pstmt.executeBatch();
                });
            }
            finally {
                statement.execute(String.format("DROP DATABASE %s", databaseName));
            }
        });
    }

    @Test
    public void testDataTypeString() throws Exception {
        withStatement(statement -> {
            String databaseName = getDatabaseName();
            String tableName = databaseName + "." + getTableName();

            try {
                statement.execute(String.format("CREATE DATABASE %s", databaseName));
                statement.execute(String.format("CREATE TABLE %s(id String)"
                        + " ENGINE=CnchMergeTree() order by tuple()", tableName));

                withPreparedStatement(String.format("INSERT INTO %s VALUES(?)", tableName), pstmt -> {
                    String valueString = "tobeInserted";
                    CharSequence valueCharSequence = "charSequence";

                    pstmt.setString(1, valueString);
                    pstmt.addBatch();

                    pstmt.setString(1, valueCharSequence.toString());
                    pstmt.addBatch();

                    pstmt.setObject(1, valueString);
                    pstmt.addBatch();

                    pstmt.setObject(1, valueCharSequence);
                    pstmt.addBatch();

                    pstmt.executeBatch();
                });
            }
            finally {
                statement.execute(String.format("DROP DATABASE %s", databaseName));
            }
        });
    }

    @Test
    public void testDataTypeFixedString() throws Exception {
        withStatement(statement -> {
            String databaseName = getDatabaseName();
            String tableName = databaseName + "." + getTableName();

            try {
                statement.execute(String.format("CREATE DATABASE %s", databaseName));
                statement.execute(String.format("CREATE TABLE %s(id FixedString(10))"
                        + " ENGINE=CnchMergeTree() order by tuple()", tableName));

                withPreparedStatement(String.format("INSERT INTO %s VALUES(?)", tableName), pstmt -> {
                    String valueString = "abcdefghij";
                    CharSequence valueCharSequence = "abcdefghij";

                    pstmt.setString(1, valueString);
                    pstmt.addBatch();

                    pstmt.setString(1, valueCharSequence.toString());
                    pstmt.addBatch();

                    pstmt.setObject(1, valueString);
                    pstmt.addBatch();

                    pstmt.setObject(1, valueCharSequence);
                    pstmt.addBatch();

                    pstmt.executeBatch();
                });
            }
            finally {
                statement.execute(String.format("DROP DATABASE %s", databaseName));
            }
        });
    }

    @Test
    public void testDataTypeIPv4() throws Exception {
        withStatement(statement -> {
            String databaseName = getDatabaseName();
            String tableName = databaseName + "." + getTableName();

            try {
                statement.execute(String.format("CREATE DATABASE %s", databaseName));
                statement.execute(String.format("CREATE TABLE %s(id IPv4)"
                        + " ENGINE=CnchMergeTree() order by tuple()", tableName));

                withPreparedStatement(String.format("INSERT INTO %s VALUES(?)", tableName), pstmt -> {
                    Long valueLong = Long.valueOf(256256256256L);
                    long valueLongPrimitive = 256256256256L;

                    pstmt.setLong(1, valueLong);
                    pstmt.addBatch();

                    pstmt.setLong(1, valueLongPrimitive);
                    pstmt.addBatch();

                    pstmt.setObject(1, valueLong);
                    pstmt.addBatch();

                    pstmt.setObject(1, valueLongPrimitive);
                    pstmt.addBatch();

                    pstmt.executeBatch();
                });
            }
            finally {
                statement.execute(String.format("DROP DATABASE %s", databaseName));
            }
        });
    }

    @Test
    public void testDataTypeIPv6() throws Exception {
        withStatement(statement -> {
            String databaseName = getDatabaseName();
            String tableName = databaseName + "." + getTableName();

            try {
                statement.execute(String.format("CREATE DATABASE %s", databaseName));
                statement.execute(String.format("CREATE TABLE %s(id IPv6)"
                        + " ENGINE=CnchMergeTree() order by tuple()", tableName));

                withPreparedStatement(String.format("INSERT INTO %s VALUES(?)", tableName), pstmt -> {
                    String valueString = "2001:44c8:129:2632:33:0:252:2";
                    Inet6Address valueInet6Address = (Inet6Address) Inet6Address.getByName("2001:44c8:129:2632:33:0:252:2");

                    pstmt.setString(1, valueString);
                    pstmt.addBatch();

                    pstmt.setString(1, valueInet6Address.getHostAddress());
                    pstmt.addBatch();

                    pstmt.setObject(1, valueString);
                    pstmt.addBatch();

                    pstmt.setObject(1, valueInet6Address);
                    pstmt.addBatch();

                    pstmt.executeBatch();
                });
            }
            finally {
                statement.execute(String.format("DROP DATABASE %s", databaseName));
            }
        });
    }

    @Test
    public void testDataTypeUUID() throws Exception {
        withStatement(statement -> {
            String databaseName = getDatabaseName();
            String tableName = databaseName + "." + getTableName();

            try {
                statement.execute(String.format("CREATE DATABASE %s", databaseName));
                statement.execute(String.format("CREATE TABLE %s(id UUID)"
                        + " ENGINE=CnchMergeTree() order by tuple()", tableName));

                withPreparedStatement(String.format("INSERT INTO %s VALUES(?)", tableName), pstmt -> {
                    String valueString = "cd175988-5fd2-11ec-bf63-0242ac130002";
                    UUID valueUUID = UUID.fromString("cd175988-5fd2-11ec-bf63-0242ac130002");

                    pstmt.setString(1, valueString);
                    pstmt.addBatch();

                    pstmt.setString(1, valueUUID.toString());
                    pstmt.addBatch();

                    pstmt.setObject(1, valueString);
                    pstmt.addBatch();

                    pstmt.setObject(1, valueUUID);
                    pstmt.addBatch();

                    pstmt.executeBatch();
                });
            }
            finally {
                statement.execute(String.format("DROP DATABASE %s", databaseName));
            }
        });
    }

    @Test
    public void testDataTypeDate() throws Exception {
        withStatement(statement -> {
            String databaseName = getDatabaseName();
            String tableName = databaseName + "." + getTableName();

            try {
                statement.execute(String.format("CREATE DATABASE %s", databaseName));
                statement.execute(String.format("CREATE TABLE %s(id Date)"
                        + " ENGINE=CnchMergeTree() order by tuple()", tableName));

                withPreparedStatement(String.format("INSERT INTO %s VALUES(?)", tableName), pstmt -> {
                    Date valueDate = new Date(25369874L);
                    LocalDate valueLocalDate = LocalDate.of(2012, 9, 9);

                    pstmt.setDate(1, valueDate);
                    pstmt.addBatch();

                    pstmt.setDate(1, Date.valueOf(valueLocalDate));
                    pstmt.addBatch();

                    pstmt.setObject(1, valueDate);
                    pstmt.addBatch();

                    pstmt.setObject(1, valueLocalDate);
                    pstmt.addBatch();

                    pstmt.executeBatch();
                });
            }
            finally {
                statement.execute(String.format("DROP DATABASE %s", databaseName));
            }
        });
    }

    @Test
    public void testDataTypeDateTime() throws Exception {
        withStatement(statement -> {
            String databaseName = getDatabaseName();
            String tableName = databaseName + "." + getTableName();

            try {
                statement.execute(String.format("CREATE DATABASE %s", databaseName));
                statement.execute(String.format("CREATE TABLE %s(id DateTime)"
                        + " ENGINE=CnchMergeTree() order by tuple()", tableName));

                withPreparedStatement(String.format("INSERT INTO %s VALUES(?)", tableName), pstmt -> {
                    ZonedDateTime valueZonedDateTime = ZonedDateTime.of(2012, 1, 1, 0, 0, 0, 0, ZoneId.of("Asia/Singapore"));
                    Timestamp valueTimestamp = Timestamp.valueOf("2012-01-01 00:00:00");

                    pstmt.setTimestamp(1, Timestamp.from(valueZonedDateTime.toInstant()));
                    pstmt.addBatch();

                    pstmt.setTimestamp(1, valueTimestamp);
                    pstmt.addBatch();

                    pstmt.setObject(1, valueZonedDateTime);
                    pstmt.addBatch();

                    pstmt.setObject(1, valueTimestamp);
                    pstmt.addBatch();

                    pstmt.executeBatch();
                });
            }
            finally {
                statement.execute(String.format("DROP DATABASE %s", databaseName));
            }
        });
    }

    @Test
    public void testDataTypeEnum8() throws Exception {
        withStatement(statement -> {
            String databaseName = getDatabaseName();
            String tableName = databaseName + "." + getTableName();

            try {
                statement.execute(String.format("CREATE DATABASE %s", databaseName));
                statement.execute(String.format("CREATE TABLE %s(id Enum8('a' = -1, 'b' = 1))"
                        + " ENGINE=CnchMergeTree() order by tuple()", tableName));

                withPreparedStatement(String.format("INSERT INTO %s VALUES(?)", tableName), pstmt -> {
                    String valueString = "a";

                    pstmt.setString(1, valueString);
                    pstmt.addBatch();

                    pstmt.executeBatch();
                });
            }
            finally {
                statement.execute(String.format("DROP DATABASE %s", databaseName));
            }
        });
    }

    @Test
    public void testDataTypeEnum16() throws Exception {
        withStatement(statement -> {
            String databaseName = getDatabaseName();
            String tableName = databaseName + "." + getTableName();

            try {
                statement.execute(String.format("CREATE DATABASE %s", databaseName));
                statement.execute(String.format("CREATE TABLE %s(id Enum16('a' = -1, 'b' = 1))"
                        + " ENGINE=CnchMergeTree() order by tuple()", tableName));

                withPreparedStatement(String.format("INSERT INTO %s VALUES(?)", tableName), pstmt -> {
                    String valueString = "a";

                    pstmt.setString(1, valueString);
                    pstmt.addBatch();

                    pstmt.executeBatch();
                });
            }
            finally {
                statement.execute(String.format("DROP DATABASE %s", databaseName));
            }
        });
    }

    @Test
    public void testDataTypeNullable() throws Exception {
        withStatement(statement -> {
            String databaseName = getDatabaseName();
            String tableName = databaseName + "." + getTableName();

            try {
                statement.execute(String.format("CREATE DATABASE %s", databaseName));
                statement.execute(String.format("CREATE TABLE %s(id Nullable(String))"
                        + " ENGINE=CnchMergeTree() order by tuple()", tableName));

                withPreparedStatement(String.format("INSERT INTO %s VALUES(?)", tableName), pstmt -> {
                    String valueString = null;

                    pstmt.setString(1, valueString);
                    pstmt.addBatch();

                    pstmt.executeBatch();
                });
            }
            finally {
                statement.execute(String.format("DROP DATABASE %s", databaseName));
            }
        });
    }

    @Test
    public void testDataTypeArray() throws Exception {
        withStatement(statement -> {
            String databaseName = getDatabaseName();
            String tableName = databaseName + "." + getTableName();

            try {
                statement.execute(String.format("CREATE DATABASE %s", databaseName));
                statement.execute(String.format("CREATE TABLE %s(id Array(Int32))"
                        + " ENGINE=CnchMergeTree() order by tuple()", tableName));

                withPreparedStatement(String.format("INSERT INTO %s VALUES(?)", tableName), pstmt -> {
                    Integer[] integers = new Integer[]{1, 2, 3};
                    int[] ints =  new int[]{1, 2, 3};

                    ByteHouseArray byteHouseArrayIntegers = new ByteHouseArray(new DataTypeInt32(), integers);
                    ByteHouseArray byteHouseArrayInts = new ByteHouseArray(new DataTypeInt32(), ints);

                    pstmt.setArray(1, byteHouseArrayIntegers);
                    pstmt.addBatch();

                    pstmt.setArray(1, byteHouseArrayInts);
                    pstmt.addBatch();

                    pstmt.setObject(1, byteHouseArrayIntegers);
                    pstmt.addBatch();

                    pstmt.setObject(1, byteHouseArrayInts);
                    pstmt.addBatch();

                    pstmt.executeBatch();
                });
            }
            finally {
                statement.execute(String.format("DROP DATABASE %s", databaseName));
            }
        });
    }

    @Test
    public void testDataTypeMap() throws Exception {
        withStatement(statement -> {
            String databaseName = getDatabaseName();
            String tableName = databaseName + "." + getTableName();

            try {
                statement.execute(String.format("CREATE DATABASE %s", databaseName));
                statement.execute(String.format("CREATE TABLE %s(id Map(Int32, Int32))"
                        + " ENGINE=CnchMergeTree() order by tuple()", tableName));

                withPreparedStatement(String.format("INSERT INTO %s VALUES(?)", tableName), pstmt -> {
                    Map<Integer, Integer> values = new HashMap<>();
                    values.put(1, 1);
                    values.put(2, 2);
                    values.put(3, 3);

                    pstmt.setObject(1, values);
                    pstmt.addBatch();

                    pstmt.executeBatch();
                });
            }
            finally {
                statement.execute(String.format("DROP DATABASE %s", databaseName));
            }
        });
    }

    @Test
    public void testDataTypeTuple() throws Exception {
        withStatement(statement -> {
            String databaseName = getDatabaseName();
            String tableName = databaseName + "." + getTableName();

            try {
                statement.execute(String.format("CREATE DATABASE %s", databaseName));
                statement.execute(String.format("CREATE TABLE %s(id Tuple(String, Int32))"
                        + " ENGINE=CnchMergeTree() order by tuple()", tableName));

                withPreparedStatement(String.format("INSERT INTO %s VALUES(?)", tableName), pstmt -> {
                    ByteHouseStruct byteHouseStruct = new ByteHouseStruct("Tuple", new Object[]{"test_string", 1});

                    pstmt.setObject(1, byteHouseStruct);
                    pstmt.addBatch();

                    pstmt.executeBatch();
                });
            }
            finally {
                statement.execute(String.format("DROP DATABASE %s", databaseName));
            }
        });
    }
}
