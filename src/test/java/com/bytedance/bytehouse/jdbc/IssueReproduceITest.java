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

package com.bytedance.bytehouse.jdbc;

import com.bytedance.bytehouse.annotation.Issue;
import com.google.common.base.Strings;
import org.junit.Ignore;

import java.sql.ResultSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class IssueReproduceITest extends AbstractITest {

    //TODO: test case infinite time running
    @Ignore
    @Issue("63")
    public void testIssue63() throws Exception {
        withStatement(statement -> {
            int columnNum = 5;
            statement.execute("DROP DATABASE IF EXISTS test_database");
            statement.execute("CREATE DATABASE test_database");
            String params = Strings.repeat("?, ", columnNum);
            StringBuilder columnTypes = new StringBuilder();
            for (int i = 0; i < columnNum; i++) {
                if (i != 0) {
                    columnTypes.append(", ");
                }
                columnTypes.append("t_").append(i).append(" String");
            }
            statement.execute("CREATE TABLE test_database.test_table( " + columnTypes + ")ENGINE=CnchMergeTree() order by tuple()");
            withPreparedStatement(statement.getConnection(), "INSERT INTO test_database.test_table values(" + params.substring(0, params.length() - 2) + ")", pstmt -> {
                for (int i = 0; i < 100; ++i) {
                    for (int j = 0; j < columnNum; j++) {
                        pstmt.setString(j + 1, "String" + j);
                    }
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            });

            ResultSet rs = statement.executeQuery("SELECT count(1) FROM test_database.test_table limit 1");
            assertTrue(rs.next());
            assertEquals(100, rs.getInt(1));

            statement.execute("DROP DATABASE test_database");
        });
    }
}
