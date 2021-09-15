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

public class AbstractSelectTBenchmark extends AbstractBenchmark {

    private static final int PRE_INSERTION_BATCH_SIZE = 10000;

    protected void initForSelect(TpcdsSpec tpcdsSpec, int selectionBatchSize) throws SQLException {
        init(tpcdsSpec.databaseName(), tpcdsSpec.tableName(), tpcdsSpec.createTableSql());
        PreparedStatement preparedStatement = getPreparedStatement(tpcdsSpec.insertSql());
        List<String[]> data = TpcdsBenchmarkSetupUtil.readDataFromCsv(tpcdsSpec.dataFile(), tpcdsSpec.dataFileColSeparator());

        int timesToInsert = (int) Math.ceil((double) selectionBatchSize / PRE_INSERTION_BATCH_SIZE);

        for (int i = 0; i < timesToInsert; i++) {
            for (int j = 0; j < PRE_INSERTION_BATCH_SIZE; j++) {
                String[] row = data.get(j % data.size());
                tpcdsSpec.addRow(preparedStatement, row);
            }
            preparedStatement.executeBatch();
        }
    }

    protected void selectRows(TpcdsSpec tpcdsSpec, int batchCount, int batchSize, Blackhole blackhole) throws SQLException {
        String selectQuery = tpcdsSpec.selectSql(batchSize);
        for (int i = 0; i < batchCount; i++) {
            blackhole.consume(getConnection().createStatement().execute(selectQuery));
        }
    }
}
