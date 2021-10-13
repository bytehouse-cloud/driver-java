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
package com.bytedance.bytehouse.helper;

import com.bytedance.bytehouse.jdbc.AbstractITest;
import org.junit.jupiter.api.Test;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;

public class Helper extends AbstractITest {
    @Test
    public void dropAllDatabases() throws Exception {

        withStatement(statement -> {
            DatabaseMetaData dm = statement.getConnection().getMetaData();
            ResultSet rs = dm.getSchemas();

            while (rs.next()) {
                String databaseName = rs.getString(1);
                try {
                    statement.execute("DROP DATABASE " + databaseName);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }
}
