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

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Types;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class ByteHouseDatabaseMetaDataITest extends AbstractITest {

    @Test
    void getTablesITest() throws Exception {
        String databaseName = getDatabaseName();
        String tableName = getTableName();
        String resourceName = databaseName + "." + tableName;

        withStatement(statement -> {
            try {
                statement.execute(String.format("CREATE DATABASE %s", databaseName));
                statement.execute(String.format("CREATE TABLE %s (id String) ENGINE=CnchMergeTree() order by tuple()", resourceName));

                DatabaseMetaData dm = statement.getConnection().getMetaData();
                ResultSet rs = dm.getTables(null, databaseName, tableName, null);

                boolean testTableFound = false;
                while (rs.next()) {
                    if (rs.getString(3).equals(tableName)) testTableFound = true;
                }
                assertTrue(testTableFound);
            }
            finally {
                getStatement(statement).execute(String.format("DROP DATABASE %s", databaseName));
            }
        });
    }

    @Test
    void getColumnsITest() throws Exception {
        String databaseName = "CAPITALIZED_DB";
        String tableName = "CAPITALIZED_TB";
        String resourceName = databaseName + "." + tableName;

        withStatement(statement -> {
            try {
                statement.execute(String.format("CREATE DATABASE %s", databaseName));
                statement.execute(String.format("CREATE TABLE %s (colString String, colInt Int32 DEFAULT 42 COMMENT 'Comment', colFloat Nullable(Float32)) "
                        + "ENGINE=CnchMergeTree() order by tuple()", resourceName));
                statement.execute(String.format("USE %s", databaseName));

                DatabaseMetaData dm = statement.getConnection().getMetaData();
                ResultSet columns = dm.getColumns(null, databaseName, tableName, null);
                assertTrue(columns.next());
                assertEquals(columns.getString("TABLE_CAT"), "default");
                assertEquals(columns.getString("TABLE_SCHEM"), databaseName.toLowerCase());
                assertEquals(columns.getString("TABLE_NAME"), tableName.toLowerCase());
                assertEquals(columns.getString("COLUMN_NAME"), "colString".toLowerCase());
                assertEquals(columns.getInt("DATA_TYPE"), Types.VARCHAR);
                assertEquals(columns.getString("TYPE_NAME"), "String");
                assertEquals(columns.getInt("COLUMN_SIZE"), 0);
                assertNull(columns.getObject("BUFFER_LENGTH"));
                assertEquals(columns.getInt("DECIMAL_DIGITS"), 0);
                assertEquals(columns.getInt("NUM_PREC_RADIX"), 10);
                assertEquals(columns.getInt("NULLABLE"), DatabaseMetaData.columnNoNulls);
                assertNull(columns.getString("REMARKS"));
                assertNull(columns.getString("COLUMN_DEF"));
                assertNull(columns.getObject("SQL_DATA_TYPE"));
                assertNull(columns.getObject("SQL_DATETIME_SUB"));
                assertEquals(columns.getInt("CHAR_OCTET_LENGTH"), 0);
                assertEquals(columns.getInt("ORDINAL_POSITION"), 1);
                assertEquals(columns.getString("IS_NULLABLE"), "NO");
                assertNull(columns.getObject("SCOPE_CATALOG"));
                assertNull(columns.getObject("SCOPE_SCHEMA"));
                assertNull(columns.getObject("SCOPE_TABLE"));
                assertNull(columns.getObject("SOURCE_DATA_TYPE"));
                assertEquals(columns.getString("IS_AUTOINCREMENT"), "NO");
                assertEquals(columns.getString("IS_GENERATEDCOLUMN"), "NO");
                assertTrue(columns.next());
                assertEquals(columns.getString("TABLE_CAT"), "default");
                assertEquals(columns.getString("TABLE_SCHEM"), databaseName.toLowerCase());
                assertEquals(columns.getString("TABLE_NAME"), tableName.toLowerCase());
                assertEquals(columns.getString("COLUMN_NAME"), "colInt".toLowerCase());
                assertEquals(columns.getInt("DATA_TYPE"), Types.INTEGER);
                assertEquals(columns.getString("TYPE_NAME"), "Int32");
                assertEquals(columns.getInt("COLUMN_SIZE"), 11);
                assertNull(columns.getObject("BUFFER_LENGTH"));
                assertEquals(columns.getInt("DECIMAL_DIGITS"), 0);
                assertEquals(columns.getInt("NUM_PREC_RADIX"), 10);
                assertEquals(columns.getInt("NULLABLE"), DatabaseMetaData.columnNoNulls);
                assertEquals(columns.getString("REMARKS"), "Comment");
                assertEquals(columns.getString("COLUMN_DEF"), "42");
                assertNull(columns.getObject("SQL_DATA_TYPE"));
                assertNull(columns.getObject("SQL_DATETIME_SUB"));
                assertEquals(columns.getInt("CHAR_OCTET_LENGTH"), 0);
                assertEquals(columns.getInt("ORDINAL_POSITION"), 2);
                assertEquals(columns.getString("IS_NULLABLE"), "NO");
                assertNull(columns.getObject("SCOPE_CATALOG"));
                assertNull(columns.getObject("SCOPE_SCHEMA"));
                assertNull(columns.getObject("SCOPE_TABLE"));
                assertNull(columns.getObject("SOURCE_DATA_TYPE"));
                assertEquals(columns.getString("IS_AUTOINCREMENT"), "NO");
                assertEquals(columns.getString("IS_GENERATEDCOLUMN"), "NO");
                assertTrue(columns.next());
                assertEquals(columns.getString("TABLE_CAT"), "default");
                assertEquals(columns.getString("TABLE_SCHEM"), databaseName.toLowerCase());
                assertEquals(columns.getString("TABLE_NAME"), tableName.toLowerCase());
                assertEquals(columns.getString("COLUMN_NAME"), "colFloat".toLowerCase());
                assertEquals(columns.getInt("DATA_TYPE"), Types.FLOAT);
                assertEquals(columns.getString("TYPE_NAME"), "Nullable(Float32)");
                assertEquals(columns.getInt("COLUMN_SIZE"), 0);
                assertNull(columns.getObject("BUFFER_LENGTH"));
                assertEquals(columns.getInt("DECIMAL_DIGITS"), 0);
                assertEquals(columns.getInt("NUM_PREC_RADIX"), 10);
                assertEquals(columns.getInt("NULLABLE"), DatabaseMetaData.columnNullable);
                assertNull(columns.getString("REMARKS"));
                assertNull(columns.getString("COLUMN_DEF"));
                assertNull(columns.getObject("SQL_DATA_TYPE"));
                assertNull(columns.getObject("SQL_DATETIME_SUB"));
                assertEquals(columns.getInt("CHAR_OCTET_LENGTH"), 0);
                assertEquals(columns.getInt("ORDINAL_POSITION"), 3);
                assertEquals(columns.getString("IS_NULLABLE"), "YES");
                assertNull(columns.getObject("SCOPE_CATALOG"));
                assertNull(columns.getObject("SCOPE_SCHEMA"));
                assertNull(columns.getObject("SCOPE_TABLE"));
                assertNull(columns.getObject("SOURCE_DATA_TYPE"));
                assertEquals(columns.getString("IS_AUTOINCREMENT"), "NO");
                assertEquals(columns.getString("IS_GENERATEDCOLUMN"), "NO");
                assertFalse(columns.next());
            }
            finally {
                getStatement(statement).execute(String.format("DROP DATABASE %s", databaseName));
            }
        });
    }

    @Test
    void getSchemasITest() throws Exception {
        String databaseName = getDatabaseName();
        String tableName = getTableName();
        String resourceName = databaseName + "." + tableName;

        withStatement(statement -> {
            try {
                statement.execute(String.format("CREATE DATABASE %s", databaseName));
                statement.execute(String.format("CREATE TABLE %s (id String) ENGINE=CnchMergeTree() order by tuple()", resourceName));

                DatabaseMetaData dm = statement.getConnection().getMetaData();
                ResultSet rs = dm.getSchemas();

                boolean testDatabaseFound = false;
                while (rs.next()) {
                    if (rs.getString(1).equals(databaseName)) testDatabaseFound = true;
                }
                assertTrue(testDatabaseFound);
            }
            finally {
                getStatement(statement).execute(String.format("DROP DATABASE %s", databaseName));
            }
        });
    }
}
