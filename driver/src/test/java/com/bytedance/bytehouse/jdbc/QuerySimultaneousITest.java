/*
 * This file may have been modified by ByteDance Ltd. and/or its affiliates.
 *
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

import org.junit.jupiter.api.Test;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test simultaneous queries with multiple data blocks using the same connection,
 * and verify that close() on ResultSet, Statement releases the resources.
 */
public class QuerySimultaneousITest extends AbstractITest {

    @Test
    public void execute_multipleQueriesSameStatement_success() throws Exception {
        withStatement(statement -> {
            ResultSet rs1 = statement.executeQuery("SELECT * FROM numbers(100)");
            // rs1 do not finish consuming all data blocks
            for (int i = 0; i < 50; i++) {
                rs1.next();
                assertEquals(rs1.getInt(1), i);
            }
            assertFalse(rs1.isClosed());
            assertTrue(rs1.next());
            // should consume remaining data blocks
            rs1.close();
            assertTrue(rs1.isClosed());
            assertFalse(rs1.next());

            // rs2 can read all 10 data blocks successfully
            ResultSet rs2 = statement.executeQuery("SELECT * FROM numbers(100)");
            for (int i = 0; i < 100; i++) {
                rs2.next();
                assertEquals(rs2.getInt(1), i);
            }
            assertFalse(rs2.next());
        }, "max_block_size", "10");
    }

    @Test
    public void execute_multipleQueriesSameStatement_autoClose() throws Exception {
        withStatement(statement -> {
            ResultSet rs1 = statement.executeQuery("SELECT * FROM numbers(100)");
            // rs1 do not finish consuming all data blocks
            for (int i = 0; i < 50; i++) {
                rs1.next();
                assertEquals(rs1.getInt(1), i);
            }
            assertFalse(rs1.isClosed());
            assertTrue(rs1.next());

            // rs2 can read all 10 data blocks successfully - statement execute will auto close previous resultset.
            ResultSet rs2 = statement.executeQuery("SELECT * FROM numbers(100)");
            for (int i = 0; i < 100; i++) {
                rs2.next();
                assertEquals(rs2.getInt(1), i);
            }

            assertTrue(rs1.isClosed());
            assertFalse(rs1.next());
            assertFalse(rs2.next());
        }, "max_block_size", "10");
    }

    @Test
    public void execute_multipleQueriesSameConnection_autoClose() throws Exception {
        withNewConnection(connection -> {
            Statement stmt1 = connection.createStatement();
            ResultSet rs1 = stmt1.executeQuery("SELECT * FROM numbers(100)");
            // rs1 do not finish consuming all data blocks
            for (int i = 0; i < 50; i++) {
                rs1.next();
                assertEquals(rs1.getInt(1), i);
            }
            assertFalse(rs1.isClosed());
            assertTrue(rs1.next());

            // statement closes its current result set.
            stmt1.close();
            assertTrue(rs1.isClosed());
            assertFalse(rs1.next());

            Statement stmt2 = connection.createStatement();
            ResultSet rs2 = stmt2.executeQuery("SELECT * FROM numbers(100)");
            for (int i = 0; i < 100; i++) {
                rs2.next();
                assertEquals(rs2.getInt(1), i);
            }
            assertFalse(rs2.next());
        }, "max_block_size", "10");
    }
}
