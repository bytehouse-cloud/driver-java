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

import com.bytedance.bytehouse.jdbc.AbstractITest;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;
import java.math.BigInteger;
import java.sql.*;

import static org.testng.Assert.*;

public class ByteHouseStatementImplTest extends AbstractITest {

    private Connection connection;

    @BeforeTest
    public void setUp() throws Exception {
        connection = getConnection();
        connection.createStatement().execute("CREATE DATABASE IF NOT EXISTS test");
    }

    @AfterTest
    public void tearDown() throws Exception {
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    public void testSingleColumnResultSet() throws SQLException {
        ResultSet rs = connection.createStatement().executeQuery("select c from (\n" +
                "    select 'a' as c, 1 as rn\n" +
                "    UNION ALL select 'b' as c, 2 as rn\n" +
                "    UNION ALL select '' as c, 3 as rn\n" +
                "    UNION ALL select 'd' as c, 4 as rn\n" +
                " ) order by rn");
        StringBuilder sb = new StringBuilder();
        while (rs.next()) {
            sb.append(rs.getString("c")).append("\n");
        }
        Assert.assertEquals(sb.toString(), "a\nb\n\nd\n");
    }

    @Test
    public void readsPastLastAreSafe() throws SQLException {
        ResultSet rs = connection.createStatement().executeQuery("select c from (\n" +
                "    select 'a' as c, 1 as rn\n" +
                "    UNION ALL select 'b' as c, 2 as rn\n" +
                "    UNION ALL select '' as c, 3 as rn\n" +
                "    UNION ALL select 'd' as c, 4 as rn\n" +
                " ) order by rn");
        StringBuilder sb = new StringBuilder();
        while (rs.next()) {
            sb.append(rs.getString("c")).append("\n");
        }
        Assert.assertFalse(rs.next());
        Assert.assertFalse(rs.next());
        Assert.assertFalse(rs.next());
        Assert.assertEquals(sb.toString(), "a\nb\n\nd\n");
    }

    @Test
    public void testSelectUInt32() throws SQLException {
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("select toUInt32(10), toUInt32(-10)");
        rs.next();
        Object smallUInt32 = rs.getObject(1);
        Assert.assertTrue(smallUInt32 instanceof Long);
        Assert.assertEquals(((Long) smallUInt32).longValue(), 10);
        Object bigUInt32 = rs.getObject(2);
        Assert.assertTrue(bigUInt32 instanceof Long);
        Assert.assertEquals(((Long) bigUInt32).longValue(), 4294967286L);
    }

    @Test
    public void testSelectUInt64() throws SQLException {
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("select toUInt64(10), toUInt64(-10)");
        rs.next();
        Object smallUInt64 = rs.getObject(1);
        Assert.assertTrue(smallUInt64 instanceof BigInteger);
        Assert.assertEquals(((BigInteger) smallUInt64).intValue(), 10);
        Object bigUInt64 = rs.getObject(2);
        Assert.assertTrue(bigUInt64 instanceof BigInteger);
        Assert.assertEquals(bigUInt64, new BigInteger("18446744073709551606"));
    }


    @Test
    public void testResultSetWithExtremes() throws SQLException {
        try {
            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery("select 1");
            StringBuilder sb = new StringBuilder();
            while (rs.next()) {
                sb.append(rs.getString(1)).append("\n");
            }

            Assert.assertEquals(sb.toString(), "1\n");
        } finally {
            connection.close();
        }
    }

    @Test
    public void testSelectOne() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            ResultSet rs = stmt.executeQuery("select\n1");
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getInt(1), 1);
            Assert.assertFalse(rs.next());
        }
    }

    @Test
    public void testSelectManyRows() throws SQLException {
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery(
            "select concat('test', toString(number)) as str from numbers(10000)");
        int i = 0;
        while (rs.next()) {
            String s = rs.getString("str");
            Assert.assertEquals(s, "test" + i);
            i++;
        }
        Assert.assertEquals(i, 10000);
    }

    @Test
    public void testMoreResultsWithResultSet() throws SQLException {
        Statement stmt = connection.createStatement();

        Assert.assertTrue(stmt.execute("select 1"));
        Assert.assertNotNull(stmt.getResultSet());
        Assert.assertEquals(stmt.getUpdateCount(), -1);

        Assert.assertFalse(stmt.getMoreResults());
        Assert.assertNull(stmt.getResultSet());
        Assert.assertEquals(stmt.getUpdateCount(), -1);
    }

    @Test
    public void testSelectQueryStartingWithWith() throws SQLException {
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("WITH 2 AS two SELECT two * two;");

        Assert.assertNotNull(rs);
        Assert.assertTrue(rs.next());
        Assert.assertEquals(rs.getInt(1), 4);

        rs.close();
        stmt.close();
    }

    // TODO: class [Ljava.lang.Object; cannot be cast to class [I ([Ljava.lang.Object; and [I are in module java.base of loader 'bootstrap')
    @Ignore
    public void testArrayMetaActualExecutiom() throws SQLException {
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT array(42, 23)");
        rs.next();
        Assert.assertEquals(rs.getMetaData().getColumnType(1), Types.ARRAY);
        Assert.assertEquals(rs.getMetaData().getColumnTypeName(1), "Array(UInt8)");
        Assert.assertEquals(rs.getMetaData().getColumnClassName(1),
            Array.class.getCanonicalName());
        Array arr = (Array) rs.getObject(1);
        Assert.assertEquals(((int[]) arr.getArray())[0], 42);
        Assert.assertEquals(((int[]) arr.getArray())[1], 23);

    }

    //TODO: ByteHouseSqlException -> no registered executor for MULTIPLE_QUERY
    @Ignore
    public void testMultiStatements() throws SQLException {
        try (Statement s = connection.createStatement()) {
            String sql = "select 1; select 2";
            try (ResultSet rs = s.executeQuery(sql)) {
                assertTrue(rs.next());
                assertEquals(rs.getString(1), "2");
                assertFalse(rs.next());
            }

            assertTrue(s.execute(sql));
            try (ResultSet rs = s.getResultSet()) {
                assertNotNull(rs);
                assertTrue(rs.next());
                assertEquals(rs.getString(1), "2");
                assertFalse(rs.next());
            }

            assertEquals(s.executeUpdate(sql), 1);
        }
    }
}
