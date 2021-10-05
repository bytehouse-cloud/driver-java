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

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

@BenchmarkMode(Mode.SingleShotTime)
public class SelectIBenchmark extends AbstractBenchmark {
    private Statement statement;
    private PreparedStatement preparedStatement;
    private String selectSql;

    @Param({"1", "10"})
    protected int batchSize = 1;

    @Param({"Int8", "Int16", "Int32", "Int64", "UInt8", "UInt16", "UInt32", "UInt64", "String", "FixedString", "UUID",
            "Float32", "Float64", "Decimal", "Date", "DateTime", "IPv4", "IPv6", "Array", "Map"})
    protected Type datatype;

    @Param({"1", "5"})
    protected int columnCount = 1;

    @Setup
    public void setup() throws Exception {
        String createTableSql = String.format("CREATE TABLE %s.%s (%s)", databaseName, tableName, getColumnsExpression(datatype, columnCount));
        init(databaseName, tableName, createTableSql);

        String insertTableSql = String.format("INSERT INTO %s.%s VALUES (%s)", databaseName, tableName, getExclaimExpression(columnCount));
        preparedStatement = getPreparedStatement(insertTableSql);
        loadData();

        selectSql = String.format("SELECT * FROM %s.%s", databaseName, tableName);
        statement = getStatement();
    }

    @Benchmark
    public void benchmarkSelect(Blackhole blackhole) throws Exception {
        blackhole.consume(statement.executeQuery(selectSql));
    }

    @TearDown
    public void teardown() throws SQLException {
        teardown(databaseName);
        getConnection().close();
    }

    private void loadData() throws SQLException {
        for (int i=0; i<batchSize; i++) {
            for (int col=1; col<=columnCount; col++) {
                preparedStatement.setObject(col, randomGenerator.getData(datatype));
            }
            preparedStatement.addBatch();
        }
        preparedStatement.executeBatch();
    }
}
