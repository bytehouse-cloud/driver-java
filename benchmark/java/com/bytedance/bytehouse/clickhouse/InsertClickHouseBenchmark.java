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

package com.bytedance.bytehouse.clickhouse;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.concurrent.atomic.AtomicInteger;
import org.openjdk.jmh.annotations.Benchmark;

public class InsertClickHouseBenchmark extends AbstractInsertClickHouseBenchmark {
    AtomicInteger tableMaxId = new AtomicInteger();
    String testDB = "test_db";

    public WithConnection benchInsert = connection -> {
        Timestamp ts = new Timestamp(System.currentTimeMillis());
        Date date = new Date(ts.getTime());

        int tableId = tableMaxId.getAndIncrement();
        String testTable = "test_" + tableId;
        String createTableSql = "CREATE TABLE " + testDB + "." + testTable + " (number UInt32, name String, birthTime DateTime, birthDay Date)";
        init(testDB, testTable, createTableSql);

        withPreparedStatement(connection,
                "INSERT INTO " + testDB + "." + testTable + " values(?, ?, ?, ?)",
                pstmt -> {
                    for (int i = 0; i < batchSize; i++) {
                        pstmt.setInt(1, i);
                        pstmt.setString(2, "i_am_a_string");
                        pstmt.setTimestamp(3, ts);
                        pstmt.setDate(4, date);
                        pstmt.addBatch();
                    }
                    int[] res = pstmt.executeBatch();
                    assertEquals(res.length, batchSize);
                }
        );

        teardown(testDB);
    };

    @Benchmark
    public void benchInsertNative() throws Exception {
        withConnection(benchInsert, ConnectionType.NATIVE);
    }
}
