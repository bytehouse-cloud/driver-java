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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import org.junit.jupiter.api.Test;

public class ResultSetMetadataITest extends AbstractITest {

    @Test
    public void successfullyMetaData() throws Exception {
        withStatement(statement -> {
            String databaseName = getDatabaseName();
            String tableName = getTableName();
            String resourceName = databaseName + "." + tableName;

            try {
                statement.execute(String.format("CREATE DATABASE %s", databaseName));
                statement.execute(String.format("CREATE TABLE %s (a UInt8, b UInt64, c FixedString(3))ENGINE=CnchMergeTree() order by tuple()", resourceName));

                statement.executeQuery(String.format("INSERT INTO %s VALUES (1, 2, '4' )", resourceName));
                ResultSet rs = statement.executeQuery(String.format("SELECT * FROM %s", resourceName));
                ResultSetMetaData metadata = rs.getMetaData();

                assertEquals(tableName, metadata.getTableName(1));
                assertEquals("default", metadata.getCatalogName(1));

                assertEquals(3, metadata.getPrecision(1));
                assertEquals(19, metadata.getPrecision(2));
                assertEquals(3, metadata.getPrecision(3));
            }
            finally {
                statement.execute(String.format("DROP DATABASE %s", databaseName));
            }
        });
    }
}
