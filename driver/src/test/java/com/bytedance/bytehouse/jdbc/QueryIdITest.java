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

import com.bytedance.bytehouse.jdbc.statement.ByteHousePreparedInsertStatement;
import com.bytedance.bytehouse.jdbc.statement.ByteHouseStatement;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class QueryIdITest extends AbstractITest {
    @Test
    public void getRandomQueryIdIfNotSet() throws Exception {
        ByteHouseStatement statement = (ByteHouseStatement) getConnection().createStatement();
        statement.execute("SELECT 1");
        String queryId1 = statement.getQueryId();
        assertNotEquals(queryId1, "");

        statement.execute("SELECT 1");
        String queryId2 = statement.getQueryId();
        assertNotEquals(queryId2, "");

        assertNotEquals(queryId1, queryId2);

        String databaseName = getDatabaseName();
        String tableName = databaseName + "." + getTableName();

        try {
            statement.execute(String.format("CREATE DATABASE IF NOT EXISTS %s", databaseName));
            statement.execute(String.format("CREATE TABLE IF NOT EXISTS %s (id INT) "
                    + "ENGINE=CnchMergeTree() order by tuple()", tableName));

            ByteHousePreparedInsertStatement pstmt = (ByteHousePreparedInsertStatement) statement.
                    getConnection().prepareStatement(String.format("INSERT INTO %s VALUES(?)", tableName));

            pstmt.setObject(1, 1);
            pstmt.addBatch();
            pstmt.executeBatch();
            String queryID1 = pstmt.getQueryId();
            assertNotEquals(queryID1, "");

            pstmt.setObject(1, 1);
            pstmt.addBatch();
            pstmt.executeBatch();
            String queryID2 = pstmt.getQueryId();
            assertNotEquals(queryID2, "");

            assertNotEquals(queryID1, queryID2);
        } finally {
            statement.execute(String.format("DROP DATABASE IF EXISTS %s", databaseName));
        }
    }

    @Test
    public void testDifferentQueryIds() throws Exception {
        ByteHouseStatement statement = (ByteHouseStatement) getConnection().createStatement();
        String[] queryIds = new String[]{"-1", "query-id", "this-is-very-long-query-id-can-you-handle-perfectly", "0000000000000"};

        for (String queryId: queryIds) {
            statement.setQueryId(queryId);
            statement.execute("SELECT 1");
            assertEquals(statement.getQueryId(), queryId);
        }

        String databaseName = getDatabaseName();
        String tableName = databaseName + "." + getTableName();

        try {
            statement.execute(String.format("CREATE DATABASE IF NOT EXISTS %s", databaseName));
            statement.execute(String.format("CREATE TABLE IF NOT EXISTS %s (id INT) "
                    + "ENGINE=CnchMergeTree() order by tuple()", tableName));

            ByteHousePreparedInsertStatement pstmt = (ByteHousePreparedInsertStatement) statement.
                    getConnection().prepareStatement(String.format("INSERT INTO %s VALUES(?)", tableName));

            for (String queryId: queryIds) {
                pstmt.setQueryId(queryId);
                pstmt.setObject(1, 1);
                pstmt.addBatch();
                pstmt.executeBatch();
                assertEquals(pstmt.getQueryId(), queryId);
            }
        } finally {
            statement.execute(String.format("DROP DATABASE IF EXISTS %s", databaseName));
        }
    }

    @Test
    public void testOnlySetLastQueryId() throws Exception {
        ByteHouseStatement statement = (ByteHouseStatement) getConnection().createStatement();
        statement.setQueryId("query-id-0");
        statement.setQueryId("query-id-1");
        statement.setQueryId("query-id-last");
        statement.execute("SELECT 1");
        assertEquals(statement.getQueryId(), "query-id-last");

        String databaseName = getDatabaseName();
        String tableName = databaseName + "." + getTableName();

        try {
            statement.execute(String.format("CREATE DATABASE IF NOT EXISTS %s", databaseName));
            statement.execute(String.format("CREATE TABLE IF NOT EXISTS %s (id INT) "
                    + "ENGINE=CnchMergeTree() order by tuple()", tableName));

            ByteHousePreparedInsertStatement pstmt = (ByteHousePreparedInsertStatement) statement.
                    getConnection().prepareStatement(String.format("INSERT INTO %s VALUES(?)", tableName));

            pstmt.setQueryId("query-id-0");
            pstmt.setQueryId("query-id-1");
            pstmt.setQueryId("query-id-last");
            pstmt.setObject(1, 1);
            pstmt.addBatch();
            pstmt.executeBatch();
            assertEquals(pstmt.getQueryId(), "query-id-last");
        } finally {
            statement.execute(String.format("DROP DATABASE IF EXISTS %s", databaseName));
        }
    }

    @Test
    public void testMultipleGetQueryId() throws Exception {
        ByteHouseStatement statement = (ByteHouseStatement) getConnection().createStatement();
        statement.setQueryId("query-id");
        statement.execute("SELECT 1");

        for (int i=0; i<5; i++) {
            assertEquals(statement.getQueryId(), "query-id");
        }

        String databaseName = getDatabaseName();
        String tableName = databaseName + "." + getTableName();

        try {
            statement.execute(String.format("CREATE DATABASE IF NOT EXISTS %s", databaseName));
            statement.execute(String.format("CREATE TABLE IF NOT EXISTS %s (id INT) "
                    + "ENGINE=CnchMergeTree() order by tuple()", tableName));

            ByteHousePreparedInsertStatement pstmt = (ByteHousePreparedInsertStatement) statement.
                    getConnection().prepareStatement(String.format("INSERT INTO %s VALUES(?)", tableName));

            pstmt.setQueryId("query-id-pstmt");
            pstmt.setObject(1, 1);
            pstmt.addBatch();
            pstmt.executeBatch();

            for (int i=0; i<5; i++) {
                assertEquals(pstmt.getQueryId(), "query-id-pstmt");
            }
        } finally {
            statement.execute(String.format("DROP DATABASE IF EXISTS %s", databaseName));
        }
    }

    @Test
    public void testEmptyQueryIdBeforeExecution() throws Exception {
        ByteHouseStatement statement = (ByteHouseStatement) getConnection().createStatement();
        assertEquals(statement.getQueryId(), "");
    }
}
