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
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

@SuppressWarnings("PMD.SystemPrintln")
public class CnchDirectQuery {

    public static void main(String[] args) throws Exception {

        final Properties properties = new Properties();

        final String url = "jdbc:cnch://10.96.36.8:9010";

        final CnchDriver driver = new CnchDriver();

        String databaseName = "inventory";
        String tableName = "orders";

        Connection connection = driver.connect(url, properties);
        createDatabase(connection);
        createTable(connection);

        insertBatch(getNewConnection(connection, driver, databaseName, tableName));
    }

    public static Connection getNewConnection(Connection connection, CnchDriver driver, String databaseName, String tableName) throws Exception {
        Connection newConnection = driver.getLatestDataSource().getConnection(connection, databaseName, tableName);
        connection.close();
        return newConnection;
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

    public static void insertBatch(Connection connection) {
        final String insertQuery = "INSERT INTO inventory.orders "
                + " (OrderID, OrderName, OrderPriority) "
                + " VALUES "
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
}
