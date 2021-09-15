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

import com.google.common.base.Strings;
import org.openjdk.jmh.annotations.Benchmark;

public class RowBinaryStringClickHouseBenchmark extends AbstractInsertClickHouseBenchmark {
    private final String columnType = "String";

    @Benchmark
    public void benchInsertNative() throws Exception {
        withConnection(connection -> {
            wideColumnPrepare(connection, columnType);
            String params = Strings.repeat("?, ", columnNum);
            withPreparedStatement(connection,
                    "INSERT INTO " + getDatabaseName() + "." + getTableName() + " values(" + params.substring(0, params.length() - 2) + ")",
                    pstmt -> {
                        for (int i = 0; i < batchSize; i++) {
                            for (int j = 0; j < columnNum; j++) {
                                pstmt.setObject(j + 1, j + 1 + "");
                            }
                            pstmt.addBatch();
                        }
                        int[] res = pstmt.executeBatch();
                        assertEquals(res.length, batchSize);
                    });
            wideColumnAfter(connection);
        }, ConnectionType.NATIVE);
    }
}