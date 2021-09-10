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
package com.bytedance.bytehouse;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;

@BenchmarkMode(Mode.AverageTime)
public class SampleBatchInsertIBenchmark extends AbstractIBenchmark {
    private String databaseName;
    private String tableName;
    private PreparedStatement preparedStatement;

    @Param({"500", "5000", "50000", "500000"})
    protected int batchSize = 1;

    @Param({"20"})
    protected String rowSizeInBytes = "";

    @Setup
    public void setup() throws Exception {
        databaseName = "sample_insert_db";
        tableName = "sample_insert_table";
        String createTableSql = String.format("CREATE TABLE %s.%s(id Int, name String, location String, age UInt8)",
                databaseName, tableName);
        init(databaseName, tableName, createTableSql);

        String insertTableSql = String.format("INSERT INTO %s.%s VALUES (?, 'Rafsan', 'Singapore', 10)", databaseName, tableName);
        preparedStatement = getPreparedStatement(insertTableSql);
    }

    @Benchmark
    public void benchmarkSampleBatchInsert(Blackhole blackhole) throws Exception {
        for (int i=0; i<batchSize; i++) {
            preparedStatement.setInt(1, i+1);
            preparedStatement.addBatch();
        }
        blackhole.consume(preparedStatement.executeBatch());
    }

    @TearDown
    public void teardown() throws SQLException {
        teardown(databaseName);
    }
}
