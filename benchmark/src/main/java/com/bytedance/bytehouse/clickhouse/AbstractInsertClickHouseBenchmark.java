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

import java.sql.Connection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
public class AbstractInsertClickHouseBenchmark extends AbstractClickHouseBenchmark {

    @Param({"20", "50"})
    protected int columnNum = 20;

    @Param({"200000", "500000"})
    protected int batchSize = 200000;

    AtomicInteger tableMaxId = new AtomicInteger();
    String testDB = "test_db";

    // drop table, create table
    protected void wideColumnPrepare(Connection connection, String columnType) throws Exception {
        int tableId = tableMaxId.incrementAndGet();
        String testTable = "test_" + tableId;
        StringBuilder createSQL = new StringBuilder("CREATE TABLE " + testDB + "." + testTable + " (");
        for (int i = 0; i < columnNum; i++) {
            createSQL.append("col_").append(i).append(" ").append(columnType);
            if (i + 1 != columnNum) {
                createSQL.append(",\n");
            }
        }
        createSQL.append(")");
        init(testDB, testTable, createSQL.toString());
    }

    protected void wideColumnAfter(Connection connection) throws Exception {
        teardown(testDB);
    }

    protected String getTableName() {
        return "test_" + tableMaxId.get();
    }

    protected String getDatabaseName() {
        return "test_db";
    }
}
