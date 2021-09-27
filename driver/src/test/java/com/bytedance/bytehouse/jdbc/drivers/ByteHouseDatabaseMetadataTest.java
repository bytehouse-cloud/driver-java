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
import java.sql.*;

public class ByteHouseDatabaseMetadataTest extends AbstractITest {

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

    // TODO: Table Schema Expected -> test, Found -> TEST
    // TODO: Table Name Expected -> testMetadata, Found -> testmetadata
    // TODO: Data type Expected -> REAL, Found -> Float
    // TODO: COLUMN_DEF -> class java.lang.String cannot be cast to class java.lang.Number
    @Ignore
    public void testMetadataColumns() throws Exception {
        connection.createStatement().executeQuery(
            "DROP TABLE IF EXISTS test.testMetadata");
        connection.createStatement().executeQuery(
            String.format("CREATE TABLE test.testMetadata("
                    + "foo Float32, bar UInt8 DEFAULT 42 COMMENT 'baz') %s", getCreateTableSuffix()));
        ResultSet columns = connection.getMetaData().getColumns(
            null, "test", "testMetadata", null);
        columns.next();
        Assert.assertEquals(columns.getString("TABLE_CAT"), "default");
        Assert.assertEquals(columns.getString("TABLE_SCHEM"), "test");
        Assert.assertEquals(columns.getString("TABLE_NAME"), "testMetadata");
        Assert.assertEquals(columns.getString("COLUMN_NAME"), "foo");
        Assert.assertEquals(columns.getInt("DATA_TYPE"), Types.REAL);
        Assert.assertEquals(columns.getString("TYPE_NAME"), "Float32");
        Assert.assertEquals(columns.getInt("COLUMN_SIZE"), 8);
        Assert.assertEquals(columns.getInt("BUFFER_LENGTH"), 0);
        Assert.assertEquals(columns.getInt("DECIMAL_DIGITS"), 8);
        Assert.assertEquals(columns.getInt("NUM_PREC_RADIX"), 10);
        Assert.assertEquals(columns.getInt("NULLABLE"), DatabaseMetaData.columnNoNulls);
        Assert.assertNull(columns.getObject("REMARKS"));
        Assert.assertNull(columns.getObject("COLUMN_DEF"));
        Assert.assertNull(columns.getObject("SQL_DATA_TYPE"));
        Assert.assertNull(columns.getObject("SQL_DATETIME_SUB"));
        Assert.assertEquals(columns.getInt("CHAR_OCTET_LENGTH"), 0);
        Assert.assertEquals(columns.getInt("ORDINAL_POSITION"), 1);
        Assert.assertEquals(columns.getString("IS_NULLABLE"), "NO");
        Assert.assertNull(columns.getObject("SCOPE_CATALOG"));
        Assert.assertNull(columns.getObject("SCOPE_SCHEMA"));
        Assert.assertNull(columns.getObject("SCOPE_TABLE"));
        Assert.assertNull(columns.getObject("SOURCE_DATA_TYPE"));
        Assert.assertEquals(columns.getString("IS_AUTOINCREMENT"), "NO");
        Assert.assertEquals(columns.getString("IS_GENERATEDCOLUMN"), "NO");
        columns.next();
        Assert.assertEquals(columns.getInt("COLUMN_DEF"), 42);
        Assert.assertEquals(columns.getObject("REMARKS"), "baz");
    }

    @Test
    public void testDatabaseVersion() throws Exception {
        String dbVersion = connection.getMetaData().getDatabaseProductVersion();
        Assert.assertFalse(dbVersion == null || dbVersion.isEmpty());
        int dbMajor = Integer.parseInt(dbVersion.substring(0, dbVersion.indexOf(".")));
        Assert.assertTrue(dbMajor > 0);
        Assert.assertEquals(connection.getMetaData().getDatabaseMajorVersion(), dbMajor);
        int majorIdx = dbVersion.indexOf(".") + 1;
        int dbMinor = Integer.parseInt(dbVersion.substring(majorIdx, dbVersion.indexOf(".", majorIdx)));
        Assert.assertEquals(connection.getMetaData().getDatabaseMinorVersion(), dbMinor);
    }

    @Test
    public void testGetTablesViews() throws Exception {
        connection.createStatement().executeQuery(
            "DROP TABLE IF EXISTS test.testMetadataView");
        connection.createStatement().executeQuery(String.format("CREATE TABLE IF NOT EXISTS test.view_table (id int) %s", getCreateTableSuffix()));
        try {
            connection.createStatement().executeQuery(
                    "CREATE VIEW IF NOT EXISTS test.testMetadataView AS SELECT 1 FROM test.view_table");
            ResultSet tableMeta = connection.getMetaData().getTables(
                    null, "test", "testMetadataView", null);
            tableMeta.next();
            Assert.assertEquals("VIEW", tableMeta.getString("TABLE_TYPE"));
        }
        finally {
            connection.createStatement().execute("DROP DATABASE IF EXISTS test");
        }
    }
}
