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
package com.bytedance.bytehouse.jdbc.drivers;

import static org.testng.Assert.assertEquals;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import com.bytedance.bytehouse.data.type.DataTypeFloat64;
import com.bytedance.bytehouse.data.type.DataTypeInt32;
import com.bytedance.bytehouse.data.type.DataTypeUInt64;
import com.bytedance.bytehouse.jdbc.AbstractITest;
import com.bytedance.bytehouse.jdbc.ByteHouseArray;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Ignore;
import ru.yandex.clickhouse.ClickHouseArray;
import ru.yandex.clickhouse.domain.ClickHouseDataType;

/**
 * Here it is assumed the connection to a ClickHouse instance with flights example data it available at localhost:8123
 * For ClickHouse quickstart and example dataset see <a href="https://clickhouse.yandex/tutorial.html">https://clickhouse.yandex/tutorial.html</a>
 */
public class ArrayTest extends AbstractITest {

    private Connection connection;

    @BeforeTest
    public void setUp() throws Exception {
        connection = getConnection();
        connection.createStatement().execute("CREATE DATABASE IF NOT EXISTS test");
    }

    // TODO: ByteHouseResultSet.getArray() does not implement the type casting for Object[]
    @Ignore
    public void testStringArray() throws SQLException {
        String[] array = {"a'','sadf',aa", "", ",", "юникод,'юникод'", ",2134,saldfk"};

        StringBuilder sb = new StringBuilder();
        for (String s : array) {
            sb.append("','").append(s.replace("'", "\\'"));
        }

        if (sb.length() > 0) {
            sb.deleteCharAt(0).deleteCharAt(0).append('\'');
        }

        String arrayString = sb.toString();

        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("select array(" + arrayString + ")");
        while (rs.next()) {
            assertEquals(rs.getArray(1).getBaseType(), Types.VARCHAR);
                    String[] stringArray = (String[]) rs.getArray(1).getArray();
                    assertEquals(stringArray.length, array.length);
                    for (int i = 0; i < stringArray.length; i++) {
                        assertEquals(stringArray[i], array[i]);
                    }
        }
        statement.close();
    }

    // TODO: ByteHouseResultSet.getArray() does not implement the type casting for Object[]
    @Ignore
    public void testLongArray() throws SQLException {
        Long[] array = {-12345678987654321L, 23325235235L, -12321342L};
        StringBuilder sb = new StringBuilder();
        for (long l : array) {
            sb.append("),toInt64(").append(l);
        }
        if (sb.length() > 0) {
            sb.deleteCharAt(0).deleteCharAt(0).append(')');
        }
        String arrayString = sb.toString();

        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("select array(" + arrayString + ")");
        while (rs.next()) {
            assertEquals(rs.getArray(1).getBaseType(), Types.BIGINT);
            long[] longArray = (long[]) rs.getArray(1).getArray();
            assertEquals(longArray.length, array.length);
            for (int i = 0; i < longArray.length; i++) {
                assertEquals(longArray[i], array[i].longValue());
            }
        }
        statement.close();
    }

    // TODO: ByteHouseResultSet.getArray() does not implement the type casting for Object[]
    @Ignore
    public void testDecimalArray() throws SQLException {
        BigDecimal[] array = {BigDecimal.valueOf(-12.345678987654321), BigDecimal.valueOf(23.325235235), BigDecimal.valueOf(-12.321342)};
        StringBuilder sb = new StringBuilder();
        for (BigDecimal d : array) {
            sb.append(", 15),toDecimal64(").append(d);
        }
        if (sb.length() > 0) {
            sb.deleteCharAt(0).delete(0, sb.indexOf(",") + 1).append(", 15)");
        }
        String arrayString = sb.toString();

        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("select array(" + arrayString + ")");
        while (rs.next()) {
            assertEquals(rs.getArray(1).getBaseType(), Types.DECIMAL);
            BigDecimal[] deciamlArray = (BigDecimal[]) rs.getArray(1).getArray();
            assertEquals(deciamlArray.length, array.length);
            for (int i = 0; i < deciamlArray.length; i++) {
                assertEquals(0, deciamlArray[i].compareTo(array[i]));
            }
        }
        statement.close();
    }

    // TODO: Bug - Insertion of arrays of UInt64 throwing type casting exception
    @Ignore
    public void testInsertUIntArray() throws SQLException {
        connection.createStatement().execute("DROP TABLE IF EXISTS test.unsigned_array");
        connection.createStatement().execute(
                String.format("CREATE TABLE IF NOT EXISTS test.unsigned_array"
                        + " (ua32 Array(UInt32), f64 Array(Float64), a32 Array(Int32)) %s", getCreateTableSuffix())
        );

        String insertSql = "INSERT INTO test.unsigned_array (ua32, f64, a32) VALUES (?, ?, ?)";

        PreparedStatement statement = connection.prepareStatement(insertSql);

        if (getDriver().equals(BYTEHOUSE)) {
            statement.setArray(1, new ByteHouseArray(new DataTypeUInt64(), new Long[]{4294967286L, 4294967287L}));
            statement.setArray(2, new ByteHouseArray(new DataTypeFloat64(), new Double[]{1.23, 4.56}));
            statement.setArray(3, new ByteHouseArray(new DataTypeInt32(), new Integer[]{-2147483648, 2147483647}));
        } else {
            statement.setArray(1, new ClickHouseArray(ClickHouseDataType.UInt64, new long[]{4294967286L, 4294967287L}));
            statement.setArray(2, new ClickHouseArray(ClickHouseDataType.Float64, new double[]{1.23, 4.56}));
            statement.setArray(3, new ClickHouseArray(ClickHouseDataType.Int32, new int[]{-2147483648, 2147483647}));
        }
        statement.execute();

        statement = connection.prepareStatement(insertSql);

        statement.setObject(1, new ArrayList<Object>(Arrays.asList(4294967286L, 4294967287L)));
        statement.setObject(2, new ArrayList<Object>(Arrays.asList(1.23, 4.56)));
        statement.setObject(3, Arrays.asList(-2147483648, 2147483647));
        statement.execute();

        Statement select = connection.createStatement();
        ResultSet rs = select.executeQuery("select ua32, f64, a32 from test.unsigned_array");
        for (int i = 0; i < 2; ++i) {
            rs.next();
            Array bigUInt32 = rs.getArray(1);
            Assert.assertEquals(bigUInt32.getBaseType(), Types.BIGINT); //
            Assert.assertEquals(bigUInt32.getArray().getClass(), long[].class);
            Assert.assertEquals(((long[]) bigUInt32.getArray())[0], 4294967286L);
            Assert.assertEquals(((long[]) bigUInt32.getArray())[1], 4294967287L);
            Array float64 = rs.getArray(2);
            Assert.assertEquals(float64.getBaseType(), Types.DOUBLE);
            Assert.assertEquals(float64.getArray().getClass(), double[].class);
            Assert.assertEquals(((double[]) float64.getArray())[0], 1.23, 0.0000001);
            Assert.assertEquals(((double[]) float64.getArray())[1], 4.56, 0.0000001);
            Array int32 = rs.getArray(3);
            Assert.assertEquals(int32.getBaseType(), Types.INTEGER); //
            Assert.assertEquals(int32.getArray().getClass(), int[].class);
            Assert.assertEquals(((int[]) int32.getArray())[0], -2147483648);
            Assert.assertEquals(((int[]) int32.getArray())[1], 2147483647);
        }
    }

    // TODO: Bug - Connection.createArrayOf() cannot parse data type properly
    @Ignore
    public void testInsertStringArray() throws Exception {
        connection.createStatement().execute("DROP TABLE IF EXISTS test.string_array");
        connection.createStatement().execute(
            String.format("CREATE TABLE IF NOT EXISTS test.string_array (foo Array(String)) %s", getCreateTableSuffix()));

        String insertSQL = "INSERT INTO test.string_array (foo) VALUES (?)";
        PreparedStatement statement = connection.prepareStatement(insertSQL);
        statement.setArray(1, connection.createArrayOf(
            String.class.getCanonicalName(),
            new String[]{"23", "42"}));
        statement.executeUpdate();

        ResultSet r = connection.createStatement().executeQuery(
            "SELECT foo FROM test.string_array");
        r.next();
        String[] s = (String[]) r.getArray(1).getArray();
        Assert.assertEquals(s[0], "23");
        Assert.assertEquals(s[1], "42");
    }

    // TODO: statement.setArray() cannot take array of java types
    @Ignore
    public void testInsertStringArrayViaUnwrap() throws Exception {
        connection.createStatement().execute("DROP TABLE IF EXISTS test.string_array");
        connection.createStatement().execute(
            String.format("CREATE TABLE IF NOT EXISTS test.string_array (foo Array(String)) %s", getCreateTableSuffix()));

        String insertSQL = "INSERT INTO test.string_array (foo) VALUES (?)";
        PreparedStatement statement;
//        if (getDriver().equals(BYTEHOUSE)) {
//            statement = connection.prepareStatement(insertSQL)
//                    .unwrap(ClickHousePreparedStatement.class);
//        } else {
//            statement = connection.prepareStatement(insertSQL)
//                    .unwrap(ByteHousePreparedInsertStatement.class);
//        }
//        statement.setArray(1, new String[] {"23", "42"});
//        statement.executeUpdate();
//
//        ResultSet r = connection.createStatement().executeQuery(
//            "SELECT foo FROM test.string_array");
//        r.next();
//        String[] s = (String[]) r.getArray(1).getArray();
//        Assert.assertEquals(s[0], "23");
//        Assert.assertEquals(s[1], "42");
    }
}
