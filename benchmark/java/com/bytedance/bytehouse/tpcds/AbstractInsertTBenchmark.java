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
package com.bytedance.bytehouse.tpcds;

import com.bytedance.bytehouse.AbstractBenchmark;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import org.openjdk.jmh.infra.Blackhole;

public class AbstractInsertTBenchmark extends AbstractBenchmark {

    protected PreparedStatement preparedStatement;
    protected List<String[]> data;

    protected void initForInsert(TpcdsSpec tpcdsSpec) throws SQLException {
        init(tpcdsSpec.databaseName(), tpcdsSpec.tableName(), tpcdsSpec.createTableSql());
        preparedStatement = getPreparedStatement(tpcdsSpec.insertSql());
        data = TpcdsBenchmarkSetupUtil.readDataFromCsv(tpcdsSpec.dataFile(), tpcdsSpec.dataFileColSeparator());
    }

    protected void insertRows(TpcdsSpec tpcdsSpec, int batchCount, int batchSize, Blackhole blackhole) throws SQLException {
        for (int i = 0; i < batchCount; i++) {
            for (int j = 0; j < batchSize; j++) {
                String[] row = data.get(j % data.size());
                tpcdsSpec.addRow(preparedStatement, row);
            }
            blackhole.consume(preparedStatement.executeBatch());
        }
    }
}
