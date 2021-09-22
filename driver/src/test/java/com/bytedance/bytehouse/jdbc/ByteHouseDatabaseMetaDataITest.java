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

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import org.junit.Ignore;

public class ByteHouseDatabaseMetaDataITest extends AbstractITest {

    @Ignore
    void getTablesITest() throws Exception {
        String databaseName = getDatabaseName();
        String tableName = getTableName();
        String resourceName = databaseName + "." + tableName;

        withStatement(statement -> {
            try {
                statement.execute(String.format("CREATE DATABASE %s", databaseName));
                statement.execute(String.format("CREATE TABLE %s (id String) ENGINE=CnchMergeTree() order by tuple()", resourceName));

                DatabaseMetaData dm = statement.getConnection().getMetaData();
                ResultSet rs = dm.getTables(null, null, null, null);

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

    @Ignore
    void getColumnsITest() throws Exception {
        String databaseName = getDatabaseName();
        String tableName = getTableName();
        String resourceName = databaseName + "." + tableName;
        String columnName = "jdbc_column_metadata_test";

        withStatement(statement -> {
            try {
                statement.execute(String.format("CREATE DATABASE %s", databaseName));
                statement.execute(String.format("CREATE TABLE %s (%s String) ENGINE=CnchMergeTree() order by tuple()", resourceName, columnName));
                statement.execute(String.format("USE %s", databaseName));

                DatabaseMetaData dm = statement.getConnection().getMetaData();
                ResultSet rs = dm.getColumns(null, null, null, null);

                boolean testColumnFound = false;
                while (rs.next()) {
                    if (rs.getString(4).equals(columnName)) testColumnFound = true;
                }
                assertTrue(testColumnFound);
            }
            finally {
                getStatement(statement).execute(String.format("DROP DATABASE %s", databaseName));
            }
        });
    }

    @Ignore
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
