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
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.util.Objects;

public class CSVInsertITest extends AbstractITest {
    public static final String curDir = Paths.get("").toAbsolutePath().toString();

    @Test
    public void testSuccessfullyCSVInsert() throws Exception {
        withStatement(statement -> {
            String databaseName = getDatabaseName();
            String tableName = databaseName + "." + getTableName();

            try {
                statement.execute(String.format("CREATE DATABASE %s", databaseName));
                statement.execute(String.format("CREATE TABLE %s(id Int32, name String, rank Float32)"
                        + " ENGINE=CnchMergeTree() order by tuple()", tableName));
                statement.execute(String.format("INSERT INTO %s FORMAT csvwithnames "
                        + "INFILE '%s/src/test/resources/csv/test.csv'", tableName, curDir));
                ResultSet rs = statement.executeQuery(String.format("SELECT * FROM %s", tableName));

                int cnt = 0;
                while (rs.next()) {
                    cnt++;
                    assert cnt != 1 || rs.getObject("name").equals("rafsan,max");
                }
                assert cnt == 10;
            }
            finally {
                statement.execute(String.format("DROP DATABASE %s", databaseName));
            }
        }, "insert_infile_local", true);
    }

    @Test
    public void testSuccessfullyCSVInsertInt() throws Exception {
        withStatement(statement -> {
            String databaseName = getDatabaseName();
            String tableName = databaseName + "." + getTableName();

            try {
                statement.execute(String.format("CREATE DATABASE %s", databaseName));
                statement.execute(String.format("CREATE TABLE %s(id8 Int8, id16 Int16, id32 Int32, id64 Int64)"
                        + " ENGINE=CnchMergeTree() order by tuple()", tableName));
                statement.execute(String.format("INSERT INTO %s FORMAT csvwithnames "
                        + "INFILE '%s/src/test/resources/csv/test_int.csv'", tableName, curDir));
                ResultSet rs = statement.executeQuery(String.format("SELECT * FROM %s", tableName));

                rs.next();
                assert rs.getByte(1) == 58;
                assert rs.getShort(2) == 1002;
                assert rs.getInt(3) == 437524212;
                assert rs.getLong(4) == Long.parseLong("437524212256898");
            }
            finally {
                statement.execute(String.format("DROP DATABASE %s", databaseName));
            }
        }, "insert_infile_local", true);
    }

    @Test
    public void testSuccessfullyCSVInsertFloat() throws Exception {
        withStatement(statement -> {
            String databaseName = getDatabaseName();
            String tableName = databaseName + "." + getTableName();

            try {
                statement.execute(String.format("CREATE DATABASE %s", databaseName));
                statement.execute(String.format("CREATE TABLE %s(id32 Float32, id64 Float64, idDecimal Decimal(20,20))"
                        + " ENGINE=CnchMergeTree() order by tuple()", tableName));
                statement.execute(String.format("INSERT INTO %s FORMAT csvwithnames "
                        + "INFILE '%s/src/test/resources/csv/test_float.csv'", tableName, curDir));
                ResultSet rs = statement.executeQuery(String.format("SELECT * FROM %s", tableName));

                rs.next();
                assert rs.getFloat(1) == 2.5;
                assert rs.getDouble(2) == 0.2568792;
                assert Objects.equals(rs.getObject(3).toString(), "32589634.54778940000000000000");
            }
            finally {
                statement.execute(String.format("DROP DATABASE %s", databaseName));
            }
        }, "insert_infile_local", true);
    }

    @Test
    public void testSuccessfullyCustomCSVDelimiterInsert() throws Exception {
        withStatement(statement -> {
            String databaseName = getDatabaseName();
            String tableName = databaseName + "." + getTableName();

            try {
                statement.execute(String.format("CREATE DATABASE %s", databaseName));
                statement.execute(String.format("CREATE TABLE %s(id Int32, name String, rank Float32)"
                        + " ENGINE=CnchMergeTree() order by tuple()", tableName));
                statement.execute(String.format("INSERT INTO %s FORMAT csvwithnames "
                        + "INFILE '%s/src/test/resources/csv/test_delimiter.csv'", tableName, curDir));
                ResultSet rs = statement.executeQuery(String.format("SELECT * FROM %s", tableName));

                int cnt = 0;
                while (rs.next()) {
                    cnt++;
                }
                assert cnt == 2;
            }
            finally {
                statement.execute(String.format("DROP DATABASE %s", databaseName));
            }
        }, "insert_infile_local", true, "format_csv_delimiter", "\\|");
    }

    @Test
    public void testSuccessfullyCSVWithoutHeaderInsert() throws Exception {
        withStatement(statement -> {
            String databaseName = getDatabaseName();
            String tableName = databaseName + "." + getTableName();

            try {
                statement.execute(String.format("CREATE DATABASE %s", databaseName));
                statement.execute(String.format("CREATE TABLE %s(name String, rank Float32, id Int32)"
                        + " ENGINE=CnchMergeTree() order by tuple()", tableName));
                statement.execute(String.format("INSERT INTO %s FORMAT csv "
                        + "INFILE '%s/src/test/resources/csv/test_no_header.csv'", tableName, curDir));
                ResultSet rs = statement.executeQuery(String.format("SELECT * FROM %s", tableName));

                int cnt = 0;
                while (rs.next()) {
                    cnt++;
                    assert cnt != 1 || rs.getObject("name").equals("rafsan,max");
                }
                assert cnt == 10;
            }
            finally {
                statement.execute(String.format("DROP DATABASE %s", databaseName));
            }
        }, "insert_infile_local", true);
    }

    @Test
    public void testSuccessfullyDateTime64CSVInsert() throws Exception {
        withStatement(statement -> {
            String databaseName = getDatabaseName();
            String tableName = databaseName + "." + getTableName();

            try {
                statement.execute(String.format("CREATE DATABASE %s", databaseName));
                statement.execute(String.format("CREATE TABLE %s(id String, datajson String, emittedat DateTime64(3, 'GMT'))"
                        + " ENGINE=CnchMergeTree() order by tuple()", tableName));
                statement.execute(String.format("INSERT INTO %s VALUES('4c2644f7-0cfc-416a-b950-2c926b9211b7', 'Sample data', '2020-10-14 01:06:29.5')", tableName));
                statement.execute(String.format("INSERT INTO %s FORMAT csv "
                        + "INFILE '%s/src/test/resources/csv/test_datetime.csv'", tableName, curDir));
                ResultSet rs = statement.executeQuery(String.format("SELECT * FROM %s", tableName));
                int consumedRows = 0;
                while (rs.next()) {
                    assert rs.getObject(1).equals("4c2644f7-0cfc-416a-b950-2c926b9211b7");
                    assert rs.getObject(2).equals("Sample data");
                    assert rs.getObject(3).toString().equals("2020-10-14 01:06:29.5");
                    consumedRows++;
                }
                assert consumedRows == 2;
             }
            finally {
                statement.execute(String.format("DROP DATABASE %s", databaseName));
            }
        }, "use_client_time_zone", "true", "insert_infile_local", true);
    }
}
