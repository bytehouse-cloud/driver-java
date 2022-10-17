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

public class ByteHouseLowCardinalityITest extends AbstractITest {

    @Test
    public void withThousandRows() throws Exception {
        withStatement(statement -> {
            try {
                statement.execute("DROP DATABASE IF EXISTS tdb");
                statement.execute("CREATE DATABASE IF NOT EXISTS tdb");
                statement.execute("CREATE TABLE IF NOT EXISTS tdb.ttable(i LowCardinality(String)) ENGINE=CnchMergeTree() ORDER BY tuple()");
                String query = "INSERT INTO tdb.ttable VALUES (?)";
                PreparedStatement preparedStatement = getConnection().prepareStatement(query);
                int SIZE = 1000;
                for (int i=1; i<=SIZE; i++) {
                    preparedStatement.setObject(1, "hello");
                    preparedStatement.addBatch();
                }
                preparedStatement.executeBatch();

                ResultSet rs = statement.executeQuery("SELECT * FROM tdb.ttable");
                int count = 0;
                while (rs.next()) {
                    assert rs.getObject(1).equals("hello");
                    count++;
                }
                assert count == SIZE;
            }
            finally {
                statement.execute("DROP DATABASE IF EXISTS tdb");
            }
        });
    }

    @Test
    public void withCustomBatchSize() throws Exception {
        withStatement(statement -> {
            try {
                statement.execute("DROP DATABASE IF EXISTS tdb");
                statement.execute("CREATE DATABASE IF NOT EXISTS tdb");
                statement.execute("CREATE TABLE IF NOT EXISTS tdb.ttable(i LowCardinality(String)) ENGINE=CnchMergeTree() ORDER BY tuple()");
                String query = "INSERT INTO tdb.ttable VALUES (?)";
                PreparedStatement preparedStatement = getConnection().prepareStatement(query);
                int SIZE = 1000;
                for (int i=1; i<=SIZE; i++) {
                    preparedStatement.setObject(1, "hello");
                    preparedStatement.addBatch();
                }
                preparedStatement.executeBatch();

                ResultSet rs = statement.executeQuery("SELECT * FROM tdb.ttable");
                int count = 0;
                while (rs.next()) {
                    assert rs.getObject(1).equals("hello");
                    count++;
                }
                assert count == SIZE;
            }
            finally {
                statement.execute("DROP DATABASE IF EXISTS tdb");
            }
        }, "max_block_size", 100);
    }

    @Test
    public void withRandomStrings() throws Exception {
        withStatement(statement -> {
            try {
                statement.execute("DROP DATABASE IF EXISTS tdb");
                statement.execute("CREATE DATABASE IF NOT EXISTS tdb");
                statement.execute("CREATE TABLE IF NOT EXISTS tdb.ttable(i LowCardinality(String)) ENGINE=CnchMergeTree() ORDER BY tuple()");
                String query = "INSERT INTO tdb.ttable VALUES (?)";
                PreparedStatement preparedStatement = getConnection().prepareStatement(query);
                int SIZE = 1000;
                for (int i=1; i<=SIZE; i++) {
                    preparedStatement.setObject(1, generateRandomString());
                    preparedStatement.addBatch();
                }
                preparedStatement.executeBatch();
            }
            finally {
                statement.execute("DROP DATABASE IF EXISTS tdb");
            }
        });
    }
}
