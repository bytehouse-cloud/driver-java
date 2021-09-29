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
package examples;

import com.bytedance.bytehouse.jdbc.CnchDriver;
import com.bytedance.bytehouse.jdbc.CnchRoutingDataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.UUID;

@SuppressWarnings("PMD.SystemPrintln")
public class CnchTxnLabelQuery {

    public static void main(String[] args) throws Exception {

//        final String url = "jdbc:cnch://default/dataexpress";
        final String url = "jdbc:cnch://post-1-0/dataexpress";
        final Properties properties = new Properties();

        try (Connection connection = new CnchDriver().connect(url, properties)) {
            createDatabase(connection);
            createTable(connection);
        } catch (SQLException ex) {
            ex.printStackTrace();
        }

        final CnchRoutingDataSource dataSource = new CnchRoutingDataSource(url, properties);

        final String s = randomLabel();
        try (Connection connection = dataSource.getConnection()) {
            insertTable(connection, s);
            selectTable(connection);
            insertBatch(connection, randomLabel());
            selectTable(connection);
        } catch (SQLException ex) {
            ex.printStackTrace();
        }

        try (Connection connection = dataSource.getConnection()) {
            insertTable(connection, s);
        } catch (SQLException ex) {
            ex.printStackTrace();
        }

        try (Connection connection = dataSource.getConnection()) {
            dropDatabase(connection);
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    public static void createDatabase(Connection connection) {
        final String sql = "CREATE DATABASE IF NOT EXISTS inventory";
        System.out.println(sql);
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(sql);
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    public static void createTable(Connection connection) {
        try (Statement stmt = connection.createStatement()) {
            final String sql = "CREATE TABLE IF NOT EXISTS inventory.orders (" +
                    " OrderID String, " +
                    " OrderName String, " +
                    " OrderPriority Int8 " +
                    " ) " +
                    " engine = CnchMergeTree()" +
                    " partition by OrderID" +
                    " order by OrderID";
            System.out.println(sql);
            stmt.execute(sql);
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    public static void insertTable(Connection connection, String label) {
        try (Statement stmt = connection.createStatement()) {
            final String sql = "INSERT INTO inventory.orders "
                    + " FORMAT Values SETTINGS insertion_label = '" + label + "' "
                    + " ('54895','Apple',12) ";
            System.out.println(sql);
            stmt.executeUpdate(sql);
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    public static void insertBatch(Connection connection, final String label) {
        final String insertQuery = "INSERT INTO inventory.orders "
                + " (OrderID, OrderName, OrderPriority) "
                + " FORMAT Values SETTINGS insertion_label = '" + label + "' "
                + " (?,'Apple',?) ";
        System.out.println(insertQuery);
        try (PreparedStatement pstmt = connection.prepareStatement(insertQuery)) {
            int insertBatchSize = 10;
            for (int i = 0; i < insertBatchSize; i++) {
                pstmt.setString(1, "ID" + i);
                pstmt.setInt(2, i);
                pstmt.addBatch();
            }
            pstmt.executeBatch();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    public static void selectTable(Connection connection) {
        final String sql = "SELECT * FROM inventory.orders";
        System.out.println(sql);
        try (Statement stmt = connection.createStatement()) {
            ResultSet rs = stmt.executeQuery(sql);
            ResultSetMetaData rsmd = rs.getMetaData();
            int columnsNumber = rsmd.getColumnCount();
            System.out.println("---------------------------------------");
            while (rs.next()) {
                for (int i = 1; i <= columnsNumber; i++) {
                    if (i > 1) System.out.print(", ");
                    String columnValue = rs.getString(i);
                    System.out.print(columnValue);
                }
                System.out.println();
            }
            System.out.println("---------------------------------------");
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    public static void dropDatabase(Connection connection) {
        final String sql = "DROP DATABASE inventory";
        System.out.println(sql);
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(sql);
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    private static String randomLabel() {
        return UUID.randomUUID().toString().replaceAll("-", "");
    }
}
