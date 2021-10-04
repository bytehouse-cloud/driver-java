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
import static org.testng.Assert.assertNotNull;

import java.math.BigInteger;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import com.bytedance.bytehouse.jdbc.AbstractITest;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;
import ru.yandex.clickhouse.ClickHousePreparedStatementImpl;

public class ByteHousePreparedStatementTest extends AbstractITest {

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
    public void testInsertUInt() throws SQLException {
        connection.createStatement().execute("DROP TABLE IF EXISTS test.unsigned_insert");
        connection.createStatement().execute(
                String.format("CREATE TABLE IF NOT EXISTS test.unsigned_insert (ui32 UInt32, ui64 UInt64) %s", getCreateTableSuffix())
        );
        PreparedStatement stmt = connection.prepareStatement("insert into test.unsigned_insert (ui32, ui64) values (?, ?)");
        stmt.setObject(1, 4294967286L);
        stmt.setObject(2, new BigInteger("18446744073709551606"));
        stmt.execute();
        Statement select = connection.createStatement();
        ResultSet rs = select.executeQuery("select ui32, ui64 from test.unsigned_insert");
        rs.next();
        Object bigUInt32 = rs.getObject(1);
        Assert.assertTrue(bigUInt32 instanceof Long);
        Assert.assertEquals(((Long) bigUInt32).longValue(), 4294967286L);
        Object bigUInt64 = rs.getObject(2);
        Assert.assertTrue(bigUInt64 instanceof BigInteger);
        Assert.assertEquals(bigUInt64, new BigInteger("18446744073709551606"));
    }

    @Test
    public void testInsertUUID() throws SQLException {
        connection.createStatement().execute("DROP TABLE IF EXISTS test.uuid_insert");
        connection.createStatement().execute(
                String.format("CREATE TABLE IF NOT EXISTS test.uuid_insert (ui32 UInt32, uuid UUID) %s", getCreateTableSuffix())
        );
        PreparedStatement stmt = connection.prepareStatement("insert into test.uuid_insert (ui32, uuid) values (?, ?)");
        stmt.setObject(1, Long.valueOf(4294967286L));
        stmt.setObject(2, UUID.fromString("bef35f40-3b03-45b0-b1bd-8ec6593dcaaa"));
        stmt.execute();
        Statement select = connection.createStatement();
        ResultSet rs = select.executeQuery("select ui32, uuid from test.uuid_insert");
        rs.next();
        Object bigUInt32 = rs.getObject(1);
        Assert.assertTrue(bigUInt32 instanceof Long);
        Assert.assertEquals(((Long) bigUInt32).longValue(), 4294967286L);
        Object uuid = rs.getObject(2);
        Assert.assertTrue(uuid instanceof UUID);
        Assert.assertEquals(uuid, UUID.fromString("bef35f40-3b03-45b0-b1bd-8ec6593dcaaa"));
    }

    @Test
    public void testInsertUUIDBatch() throws SQLException {
        connection.createStatement().execute("DROP TABLE IF EXISTS test.uuid_insert");
        connection.createStatement().execute(
                String.format("CREATE TABLE IF NOT EXISTS test.uuid_insert (ui32 UInt32, uuid UUID) %s", getCreateTableSuffix())
        );
        PreparedStatement stmt = connection.prepareStatement("insert into test.uuid_insert (ui32, uuid) values (?, ?)");
        stmt.setObject(1, 4294967286L);
        stmt.setObject(2, UUID.fromString("bef35f40-3b03-45b0-b1bd-8ec6593dcaaa"));
        stmt.addBatch();
        stmt.executeBatch();
        Statement select = connection.createStatement();
        ResultSet rs = select.executeQuery("select ui32, uuid from test.uuid_insert");
        rs.next();
        Object bigUInt32 = rs.getObject(1);
        Assert.assertTrue(bigUInt32 instanceof Long);
        Assert.assertEquals(((Long) bigUInt32).longValue(), 4294967286L);
        Object uuid = rs.getObject(2);
        Assert.assertTrue(uuid instanceof UUID);
        Assert.assertEquals(uuid, UUID.fromString("bef35f40-3b03-45b0-b1bd-8ec6593dcaaa"));
    }

    @Test
    public void testInsertStringContainsKeyword() throws SQLException {
        connection.createStatement().execute("DROP TABLE IF EXISTS test.keyword_insert");
        connection.createStatement().execute(
                String.format("CREATE TABLE test.keyword_insert(a String,b String) %s", getCreateTableSuffix())
        );

        PreparedStatement stmt = connection.prepareStatement("insert into test.keyword_insert(a,b) values('values(',',')");
        stmt.execute();
        
        Statement select = connection.createStatement();
        ResultSet rs = select.executeQuery("select * from test.keyword_insert");
        Assert.assertTrue(rs.next());
        Assert.assertEquals(rs.getString(1), "values(");
        Assert.assertEquals(rs.getString(2), ",");
        Assert.assertFalse(rs.next());
    }

    @Test
    public void testInsertNullString() throws SQLException {
        connection.createStatement().execute("DROP TABLE IF EXISTS test.null_insert");
        connection.createStatement().execute(
                String.format("CREATE TABLE IF NOT EXISTS test.null_insert (val Nullable(String)) %s", getCreateTableSuffix())
        );

        PreparedStatement stmt = connection.prepareStatement("insert into test.null_insert (val) values (?)");
        stmt.setNull(1, Types.VARCHAR);
        stmt.execute();
        stmt.setNull(1, Types.VARCHAR);
        stmt.addBatch();
        stmt.executeBatch();

        stmt.setString(1, null);
        stmt.execute();
        stmt.setString(1, null);
        stmt.addBatch();
        stmt.executeBatch();

        stmt.setObject(1, null);
        stmt.execute();
        stmt.setObject(1, null);
        stmt.addBatch();
        stmt.executeBatch();

        Statement select = connection.createStatement();
        ResultSet rs = select.executeQuery("select count(*), val from test.null_insert group by val");
        rs.next();
        Assert.assertEquals(rs.getInt(1), 6);
        Assert.assertNull(rs.getString(2));
        Assert.assertFalse(rs.next());
    }

    // TODO: Metdata types: expected [7] but found [6]
    @Ignore
    public void testSelectNullableTypes() throws SQLException {
        connection.createStatement().execute("DROP TABLE IF EXISTS test.select_nullable");
        connection.createStatement().execute(
                String.format("CREATE TABLE IF NOT EXISTS test.select_nullable (idx Int32, i Nullable(Int32), ui Nullable(UInt64), f Nullable(Float32), s Nullable(String)) %s", getCreateTableSuffix())
        );

        PreparedStatement stmt = connection.prepareStatement("insert into test.select_nullable (idx, i, ui, f, s) values (?, ?, ?, ?, ?)");
        stmt.setInt(1, 1);
        stmt.setObject(2, null);
        stmt.setObject(3, null);
        stmt.setObject(4, null);
        stmt.setString(5, null);
        stmt.addBatch();
        stmt.setInt(1, 2);
        stmt.setInt(2, 1);
        stmt.setInt(3, 1);
        stmt.setFloat(4, 1.0f);
        stmt.setString(5, "aaa");
        stmt.addBatch();
        stmt.executeBatch();

        Statement select = connection.createStatement();
        ResultSet rs = select.executeQuery("select i, ui, f, s from test.select_nullable order by idx");
        rs.next();
        Assert.assertEquals(rs.getMetaData().getColumnType(1), Types.INTEGER);
        Assert.assertEquals(rs.getMetaData().getColumnType(2), Types.BIGINT);
        Assert.assertEquals(rs.getMetaData().getColumnType(3), Types.REAL);
        Assert.assertEquals(rs.getMetaData().getColumnType(4), Types.VARCHAR);

        Assert.assertNull(rs.getObject(1));
        Assert.assertNull(rs.getObject(2));
        Assert.assertNull(rs.getObject(3));
        Assert.assertNull(rs.getObject(4));

        Assert.assertEquals(rs.getInt(1), 0);
        Assert.assertEquals(rs.getInt(1), 0);
        Assert.assertEquals(rs.getFloat(1), 0.0f);
        Assert.assertEquals(rs.getString(1), null);

        rs.next();
        Assert.assertEquals(rs.getObject(1).getClass(), Integer.class);
        Assert.assertEquals(rs.getObject(2).getClass(), BigInteger.class);
        Assert.assertEquals(rs.getObject(3).getClass(), Float.class);
        Assert.assertEquals(rs.getObject(4).getClass(), String.class);

        Assert.assertEquals(rs.getObject(1), 1);
        Assert.assertEquals(rs.getObject(2), BigInteger.ONE);
        Assert.assertEquals(rs.getObject(3), 1.0f);
        Assert.assertEquals(rs.getObject(4), "aaa");

    }

    //TODO: VALUES () () does not support
    @Ignore
    public void testInsertBatchNullValues() throws Exception {
        connection.createStatement().execute(
            "DROP TABLE IF EXISTS test.prep_nullable_value");
        connection.createStatement().execute(
            String.format(            "CREATE TABLE IF NOT EXISTS test.prep_nullable_value "
                    + "(idx Int32, s Nullable(String), i Nullable(Int32), f Nullable(Float32)) "
                    + "%s", getCreateTableSuffix())
        );
        PreparedStatement stmt = connection.prepareStatement(
            "INSERT INTO test.prep_nullable_value (idx, s, i, f) VALUES "
          + "(1, ?, ?, NULL), (2, NULL, NULL, ?)");
        stmt.setString(1, "foo");
        stmt.setInt(2, 42);
        stmt.setFloat(3, 42.0F);
        stmt.addBatch();
        int[] updateCount = stmt.executeBatch();
        Assert.assertEquals(updateCount.length, 2);

        ResultSet rs = connection.createStatement().executeQuery(
            "SELECT s, i, f FROM test.prep_nullable_value "
          + "ORDER BY idx ASC");
        rs.next();
        Assert.assertEquals(rs.getString(1), "foo");
        Assert.assertEquals(rs.getInt(2), 42);
        Assert.assertNull(rs.getObject(3));
        rs.next();
        Assert.assertNull(rs.getObject(1));
        Assert.assertNull(rs.getObject(2));
        Assert.assertEquals(rs.getFloat(3), 42.0f);
    }

    @Test
    public void testSelectDouble() throws SQLException {
        Statement select = connection.createStatement();
        ResultSet rs = select.executeQuery("select toFloat64(0.1) ");
        rs.next();
        Assert.assertEquals(rs.getMetaData().getColumnType(1), Types.DOUBLE);
        Assert.assertEquals(rs.getObject(1).getClass(), Double.class);
        Assert.assertEquals(rs.getDouble(1), 0.1);
    }

    @Test
    public void clickhouseJdbcFailsBecauseOfCommentInStart() throws Exception {
        String sqlStatement = "/*comment*/ select * from numbers(3)";
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery(sqlStatement);
        Assert.assertNotNull(rs);
        for (int i = 0; i < 3; i++) {
            rs.next();
            Assert.assertEquals(rs.getInt(1), i);
        }
    }

    @Test
    public void testTrailingParameterOrderBy() throws Exception {
        String sqlStatement =
            "SELECT 42 AS foo, 23 AS bar from numbers(100) "
          + "ORDER BY foo DESC LIMIT ?, ?";
        PreparedStatement stmt = connection.prepareStatement(sqlStatement);
        stmt.setInt(1, 23);
        stmt.setInt(2, 42);
        ResultSet rs = stmt.executeQuery();
        Assert.assertTrue(rs.next());
    }

    // PreparedStatement.getMetaData() returns null
    @Ignore
    public void testMetadataOnlySelect() throws Exception {
        connection.createStatement().execute(
            "DROP TABLE IF EXISTS test.mymetadata");
        connection.createStatement().execute(
            String.format(            "CREATE TABLE IF NOT EXISTS test.mymetadata "
                    + "(idx Int32, s String) "
                    + "%s", getCreateTableSuffix())
        );
        PreparedStatement insertStmt = connection.prepareStatement(
            "INSERT INTO test.mymetadata (idx, s) VALUES (?, ?)");
        insertStmt.setInt(1, 42);
        insertStmt.setString(2, "foo");
        insertStmt.executeUpdate();
        PreparedStatement metaStmt = connection.prepareStatement(
            "SELECT idx, s FROM test.mymetadata WHERE idx = ?");
        metaStmt.setInt(1, 42);
        ResultSetMetaData metadata = metaStmt.getMetaData();
        Assert.assertEquals(metadata.getColumnCount(), 2);
        Assert.assertEquals(metadata.getColumnName(1), "idx");
        Assert.assertEquals(metadata.getColumnName(2), "s");
    }

    @Test
    public void testMetadataOnlySelectAfterExecution() throws Exception {
        connection.createStatement().execute(
            "DROP TABLE IF EXISTS test.mymetadata");
        connection.createStatement().execute(
            String.format(            "CREATE TABLE IF NOT EXISTS test.mymetadata "
                    + "(idx Int32, s String) "
                    + "%s", getCreateTableSuffix())
        );
        PreparedStatement insertStmt = connection.prepareStatement(
            "INSERT INTO test.mymetadata (idx, s) VALUES (?, ?)");
        insertStmt.setInt(1, 42);
        insertStmt.setString(2, "foo");
        insertStmt.executeUpdate();
        PreparedStatement metaStmt = connection.prepareStatement(
            "SELECT idx, s FROM test.mymetadata WHERE idx = ?");
        metaStmt.setInt(1, 42);
        metaStmt.executeQuery();
        ResultSetMetaData metadata = metaStmt.getMetaData();
        Assert.assertEquals(metadata.getColumnCount(), 2);
        Assert.assertEquals(metadata.getColumnName(1), "idx");
        Assert.assertEquals(metadata.getColumnName(2), "s");
    }

    // PreparedStatement.getMetaData() returns null
    @Ignore
    public void testMetadataExecutionAfterMeta() throws Exception {
        connection.createStatement().execute(
            "DROP TABLE IF EXISTS test.mymetadata");
        connection.createStatement().execute(
            String.format(            "CREATE TABLE IF NOT EXISTS test.mymetadata "
                    + "(idx Int32, s String) "
                    + "%s", getCreateTableSuffix())
        );
        PreparedStatement insertStmt = connection.prepareStatement(
            "INSERT INTO test.mymetadata (idx, s) VALUES (?, ?)");
        insertStmt.setInt(1, 42);
        insertStmt.setString(2, "foo");
        insertStmt.executeUpdate();
        PreparedStatement metaStmt = connection.prepareStatement(
            "SELECT idx, s FROM test.mymetadata WHERE idx = ?");
        metaStmt.setInt(1, 42);
        ResultSetMetaData metadata = metaStmt.getMetaData();
        Assert.assertEquals(metadata.getColumnCount(), 2);
        Assert.assertEquals(metadata.getColumnName(1), "idx");
        Assert.assertEquals(metadata.getColumnName(2), "s");

        ResultSet rs = metaStmt.executeQuery();
        Assert.assertTrue(rs.next());
        Assert.assertEquals(rs.getInt(1), 42);
        Assert.assertEquals(rs.getString(2), "foo");
        metadata = metaStmt.getMetaData();
        Assert.assertEquals(metadata.getColumnCount(), 2);
        Assert.assertEquals(metadata.getColumnName(1), "idx");
        Assert.assertEquals(metadata.getColumnName(2), "s");
    }

    // PreparedStatement.getMetaData() returns null
    @Ignore
    public void testMetadataOnlyUpdate() throws Exception {
        connection.createStatement().execute(
            "DROP TABLE IF EXISTS test.mymetadata");
        connection.createStatement().execute(
            String.format(            "CREATE TABLE IF NOT EXISTS test.mymetadata "
                    + "(idx Int32, s String) "
                    + "%s", getCreateTableSuffix())
        );
        PreparedStatement insertStmt = connection.prepareStatement(
            "INSERT INTO test.mymetadata (idx, s) VALUES (?, ?)");
        insertStmt.setInt(1, 42);
        insertStmt.setString(2, "foo");
        insertStmt.executeUpdate();
        PreparedStatement metaStmt = connection.prepareStatement(
            "UPDATE test.mymetadata SET s = ? WHERE idx = ?");
        metaStmt.setString(1, "foo");
        metaStmt.setInt(2, 42);
        ResultSetMetaData metadata = metaStmt.getMetaData();
        Assert.assertNull(metadata);
        metaStmt.close();
    }

    // TODO: Cannot parse expression in insert statement
    @Ignore
    public void testInsertWithFunctions() throws Exception {
        connection.createStatement().execute(
            "DROP TABLE IF EXISTS test.insertfunctions");
        connection.createStatement().execute(
            String.format(            "CREATE TABLE IF NOT EXISTS test.insertfunctions "
                    + "(id UInt32, foo String, bar String) "
                    + "%s", getCreateTableSuffix())
        );
        PreparedStatement stmt = connection.prepareStatement(
            "INSERT INTO test.insertfunctions(id, foo, bar) VALUES "
          + "(?, lower(reverse(?)), upper(reverse(?)))");
        stmt.setInt(1, 42);
        stmt.setString(2, "Foo");
        stmt.setString(3, "Bar");
        String sql = stmt.unwrap(ClickHousePreparedStatementImpl.class).asSql();
        Assert.assertEquals(
            sql,
            "INSERT INTO test.insertfunctions(id, foo, bar) VALUES "
          + "(42, lower(reverse('Foo')), upper(reverse('Bar')))");
        // make sure that there is no exception
        stmt.execute();
        ResultSet rs = connection.createStatement().executeQuery(
            "SELECT id, foo, bar FROM test.insertfunctions");
        rs.next();
        Assert.assertEquals(rs.getInt(1), 42);
        Assert.assertEquals(rs.getString(2), "oof");
        Assert.assertEquals(rs.getString(3), "RAB");
        rs.close();
    }

    @Test
    public void testBytes() throws Exception {
        connection.createStatement().execute(
            "DROP TABLE IF EXISTS test.strings_versus_bytes");
        connection.createStatement().execute(
            String.format(            "CREATE TABLE IF NOT EXISTS test.strings_versus_bytes"
                    + "(s String, fs FixedString(8)) "
                    + "%s", getCreateTableSuffix())
        );
        PreparedStatement insertStmt = connection.prepareStatement(
            "INSERT INTO test.strings_versus_bytes (s, fs) VALUES (?, ?)");
        insertStmt.setBytes(1, "foo".getBytes(Charset.forName("UTF-8")));
        insertStmt.setBytes(2, "bar".getBytes(Charset.forName("UTF-8")));
        insertStmt.executeUpdate();
        ResultSet rs = connection.createStatement().executeQuery(
            "SELECT s, fs FROM test.strings_versus_bytes");
        rs.next();
        Assert.assertEquals(rs.getString(1), "foo");
        // TODO: The actual String returned by our ResultSet is rather strange
        // ['b' 'a' 'r' 0 0 0 0 0]
        Assert.assertEquals(rs.getString(2).trim(), "bar");
    }

    // TODO: Cannot parse expression in insert statement
    @Ignore
    public void testInsertWithFunctionsAddBatch() throws Exception {
        connection.createStatement().execute(
            "DROP TABLE IF EXISTS test.insertfunctions");
        connection.createStatement().execute(
            "CREATE TABLE IF NOT EXISTS test.insertfunctions "
          + "(id UInt32, foo String, bar String) "
          + "ENGINE = TinyLog");
        PreparedStatement stmt = connection.prepareStatement(
            "INSERT INTO test.insertfunctions(id, foo, bar) VALUES "
          + "(?, lower(reverse(?)), upper(reverse(?)))");
        stmt.setInt(1, 42);
        stmt.setString(2, "Foo");
        stmt.setString(3, "Bar");
        stmt.addBatch();
        stmt.executeBatch();
        // this will _not_ perform the functions, but instead send the parameters
        // as is to the clickhouse server
    }

    @SuppressWarnings("boxing")
    // TODO: Don't support VALUES () ()
    @Ignore
    public void testMultiLineValues() throws Exception {
        connection.createStatement().execute(
            "DROP TABLE IF EXISTS test.multiline");
        connection.createStatement().execute(
            String.format(            "CREATE TABLE IF NOT EXISTS test.multiline"
                    + "(foo Int32, bar String) "
                    + "%s", getCreateTableSuffix())
        );
        PreparedStatement insertStmt = connection.prepareStatement(
            "INSERT INTO test.multiline\n"
          + "\t(foo, bar)\r\n"
          + "\t\tVALUES\n"
          + "(?, ?) , \n\r"
          + "\t(?,?),(?,?)\n");
        Map<Integer, String> testData = new HashMap<>();
        testData.put(23, "baz");
        testData.put(42, "bar");
        testData.put(1337, "oof");
        int i = 0;
        for (Integer k : testData.keySet()) {
            insertStmt.setInt(++i, k.intValue());
            insertStmt.setString(++i, testData.get(k));
        }
        insertStmt.executeUpdate();

        ResultSet rs = connection.createStatement().executeQuery(
            "SELECT * FROM test.multiline ORDER BY foo");
        rs.next();
        Assert.assertEquals(rs.getInt(1), 23);
        Assert.assertEquals(rs.getString(2), "baz");
        rs.next();
        Assert.assertEquals(rs.getInt(1), 42);
        Assert.assertEquals(rs.getString(2), "bar");
        rs.next();
        Assert.assertEquals(rs.getInt(1), 1337);
        Assert.assertEquals(rs.getString(2), "oof");
        Assert.assertFalse(rs.next());
    }

    // Issue 153
    // TODO: getArray() cannot convert to array of types
    @Ignore
    public void testArrayDateTime() throws Exception {
        connection.createStatement().execute(
            "DROP TABLE IF EXISTS test.date_time_array");
        connection.createStatement().execute(
            String.format(            "CREATE TABLE IF NOT EXISTS test.date_time_array"
                    + "(foo Array(DateTime)) "
                    + "%s", getCreateTableSuffix())
        );
        PreparedStatement stmt = connection.prepareStatement(
            "INSERT INTO test.date_time_array (foo) VALUES (?)");
        stmt.setArray(1, connection.createArrayOf("DateTime",
            new Timestamp[] {
                new Timestamp(1557136800000L),
                new Timestamp(1560698526598L)
            }));
        stmt.execute();

        ResultSet rs = connection.createStatement().executeQuery(
            "SELECT foo FROM test.date_time_array");
        rs.next();
        Timestamp[] result = (Timestamp[]) rs.getArray(1).getArray();
        Assert.assertEquals(result[0].getTime(), 1557136800000L);
        Assert.assertEquals(result[1].getTime(), 1560698526598L);
    }

    @Test
    public void testStaticNullValue() throws Exception {
        connection.createStatement().execute(
            "DROP TABLE IF EXISTS test.static_null_value");
        connection.createStatement().execute(
            String.format(            "CREATE TABLE IF NOT EXISTS test.static_null_value"
                    + "(foo Nullable(String), bar Nullable(String)) "
                    + "%s", getCreateTableSuffix())
        );
        PreparedStatement ps0 = connection.prepareStatement(
            "INSERT INTO test.static_null_value(foo) VALUES (null)");
        ps0.executeUpdate();

        ps0 = connection.prepareStatement(
            "INSERT INTO test.static_null_value(foo, bar) VALUES (null, ?)");
        ps0.setNull(1, Types.VARCHAR);
        ps0.executeUpdate();
    }

    // TODO: addBatch() missing implementations
    @Ignore
    public void testBatchProcess() throws Exception {
        connection.createStatement().execute("drop table if exists test.batch_update");
        try (PreparedStatement s = connection.prepareStatement(
            String.format("create table if not exists test.batch_update(k UInt8, v String) %s", getCreateTableSuffix()))) {
            s.execute();
        }

        Object[][] data = new Object[][] {
            new Object[] {1, "a"},
            new Object[] {1, "b"},
            new Object[] {3, "c"}
        };

        // insert
        try (PreparedStatement s = connection.prepareStatement("insert into table test.batch_update values(?,?)")) {
            for (int i = 0; i < data.length; i++) {
                Object[] row = data[i];
                s.setInt(1, (int) row[0]);
                s.setString(2, (String) row[1]);
                s.addBatch();
            }
            int[] results = s.executeBatch();
            assertNotNull(results);
            assertEquals(results.length, 3);
        }

        // select
        try (PreparedStatement s = connection.prepareStatement(
            "select * from test.batch_update where k in (?, ?) order by k, v")) {
            s.setInt(1, 1);
            s.setInt(2, 3);
            ResultSet rs = s.executeQuery();
            int index = 0;
            while (rs.next()) {
                Object[] row = data[index++];
                assertEquals(rs.getInt(1), (int) row[0]);
                assertEquals(rs.getString(2), (String) row[1]);
            }
            assertEquals(index, data.length);
        }

        // update
        try (PreparedStatement s = connection.prepareStatement(
            "alter table test.batch_update update v = ? where k = ?")) {
            s.setString(1, "x");
            s.setInt(2, 1);
            s.addBatch();
            s.setString(1, "y");
            s.setInt(2, 3);
            s.addBatch();
            int[] results = s.executeBatch();
            assertNotNull(results);
            assertEquals(results.length, 2);
        }

        // delete
        try (PreparedStatement s = connection.prepareStatement("alter table test.batch_update delete where k = ?")) {
            s.setInt(1, 1);
            s.addBatch();
            s.setInt(1, 3);
            s.addBatch();
            int[] results = s.executeBatch();
            assertNotNull(results);
            assertEquals(results.length, 2);
        }

        try (PreparedStatement s = connection.prepareStatement("drop table if exists test.batch_update")) {
            s.execute();
        }
    }
}
