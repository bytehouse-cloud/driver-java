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
package com.bytedance.bytehouse.jdbc.drivers;

import com.bytedance.bytehouse.jdbc.AbstractITest;
import org.junit.Ignore;
import java.sql.ResultSet;
import java.time.LocalDate;

import static org.junit.jupiter.api.Assertions.*;

public class InsertComplexExpressionITest extends AbstractITest {
    /*
        Bytehouse driver - Not supported
        Official clickhouse driver - supported
        What to implement - Parser for insert query expressions
     */
    @Ignore
    public void insertToDateExpression() throws Exception {
        withStatement(statement -> {
            String databaseName = getDatabaseName();
            String tableName = databaseName + "." + getTableName();

            try {
                statement.execute(String.format("CREATE DATABASE %s", databaseName));
                statement.execute(String.format("CREATE TABLE %s (test Date) %s", tableName, getCreateTableSuffix()));

                statement.executeQuery(String.format("INSERT INTO %s VALUES(toDate('2000-01-01'))", tableName));
                ResultSet rs = statement.executeQuery(String.format("SELECT * FROM %s", tableName));
                assertTrue(rs.next());
                assertEquals(
                        LocalDate.of(2000, 1, 1).toEpochDay(),
                        rs.getDate(1).toLocalDate().toEpochDay());

                assertFalse(rs.next());
            }
            finally {
                statement.execute(String.format("DROP DATABASE %s", databaseName));
            }
        });
    }
}
