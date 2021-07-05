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
