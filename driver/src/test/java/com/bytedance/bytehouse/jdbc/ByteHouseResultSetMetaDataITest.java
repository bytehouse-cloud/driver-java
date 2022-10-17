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

import org.junit.jupiter.api.Test;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ByteHouseResultSetMetaDataITest extends AbstractITest {

    @Test
    public void testNonNullMetadataBeforeExecution() throws Exception {
        withStatement(stmt -> {
            String databaseName = getDatabaseName();
            String tableName = databaseName + "." + getTableName();

            try {
                stmt.execute(String.format("CREATE DATABASE %s", databaseName));
                stmt.execute(String.format("CREATE TABLE %s(id Int)"
                        + " ENGINE=CnchMergeTree() order by tuple()", tableName));

                stmt.execute(String.format("INSERT INTO %s VALUES(1)", tableName));
                stmt.execute(String.format("INSERT INTO %s VALUES(2)", tableName));
                stmt.execute(String.format("INSERT INTO %s VALUES(3)", tableName));

                PreparedStatement preparedStatement = getConnection().prepareStatement(String.format("SELECT * FROM %s", tableName));
                ResultSetMetaData metaData = preparedStatement.getMetaData();
                assertNotNull(metaData);
                assertEquals(1, metaData.getColumnCount());
                assertEquals("id", metaData.getColumnName(1));
                preparedStatement.execute();
                metaData = preparedStatement.getMetaData();
                assertNotNull(metaData);
                assertEquals(1, metaData.getColumnCount());
                assertEquals("id", metaData.getColumnName(1));

                List<Integer> arrayList = new ArrayList<>();
                ResultSet resultSet = preparedStatement.getResultSet();
                resultSet.next();
                arrayList.add((Integer) resultSet.getObject(1));
                metaData = preparedStatement.getMetaData();
                assertNotNull(metaData);
                assertEquals(1, metaData.getColumnCount());
                assertEquals("id", metaData.getColumnName(1));
                resultSet.next();
                arrayList.add((Integer) resultSet.getObject(1));
                resultSet.next();
                arrayList.add((Integer) resultSet.getObject(1));

                Collections.sort(arrayList);
                for (int i=0; i<arrayList.size(); i++) {
                    assertEquals(arrayList.get(i), i+1);
                }
            }
            finally {
                stmt.execute(String.format("DROP DATABASE %s", databaseName));
            }
        });
    }

    @Test
    void testColumnMethods() throws Exception {
        withNewConnection(connection -> {
            ByteHouseResultSet rs = ByteHouseResultSetBuilder
                                         .builder(8, ((ByteHouseConnection) connection).serverContext())
                                         .cfg(((ByteHouseConnection) connection).cfg())
                                         .columnNames("a1", "a2", "a3", "a4", "a5", "a6", "a7", "a8")
                                         .columnTypes("String", "UInt32", "Int64", "Float32", "Float64", "Decimal(76, 26)",
                                                 "Nullable(Int64)", "Nullable(UInt64)")
                                         .build();

            assertEquals("a1", rs.getMetaData().getColumnName(1));

            assertFalse(rs.getMetaData().isSigned(2));
            assertTrue(rs.getMetaData().isSigned(3));
            assertTrue(rs.getMetaData().isSigned(4));
            assertTrue(rs.getMetaData().isSigned(5));
            assertTrue(rs.getMetaData().isSigned(7));
            assertFalse(rs.getMetaData().isSigned(8));

            assertEquals(8, rs.getMetaData().getPrecision(4));
            assertEquals(17, rs.getMetaData().getPrecision(5));
            assertEquals(76, rs.getMetaData().getPrecision(6));

            assertEquals(8, rs.getMetaData().getScale(4));
            assertEquals(17, rs.getMetaData().getScale(5));
            assertEquals(26, rs.getMetaData().getScale(6));

            assertEquals("a4", rs.getMetaData().getColumnName(4));
            assertEquals("Float64", rs.getMetaData().getColumnTypeName(5));
            assertEquals(Types.DECIMAL, rs.getMetaData().getColumnType(6));

            assertFalse(rs.next());
        });
    }
}
