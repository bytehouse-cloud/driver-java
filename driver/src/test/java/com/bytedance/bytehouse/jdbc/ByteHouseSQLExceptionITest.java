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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.bytedance.bytehouse.exception.ByteHouseSQLException;
import java.sql.SQLException;
import org.junit.jupiter.api.Test;


public class ByteHouseSQLExceptionITest extends AbstractITest {

    @Test
    public void errorCodeShouldBeAssigned() throws Exception {
        withStatement(statement -> {
            try {
                statement.executeQuery("DROP TABLE test");
            } catch (SQLException e) {
                assertTrue(e instanceof ByteHouseSQLException);
                assertEquals(0, e.getErrorCode()); // gateway default error code
                assertEquals(e.getErrorCode(), e.getErrorCode());
            }
        });
    }
}
