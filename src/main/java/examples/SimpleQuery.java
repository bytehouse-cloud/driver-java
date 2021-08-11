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

import com.bytedance.bytehouse.jdbc.BalancedByteHouseDataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import javax.sql.DataSource;

/**
 * This simple query demonstrate basic steps to create table and insert data and query data.
 */
public class SimpleQuery {

    public static void main(String[] args) throws Exception {
        final String url = String.format("jdbc:bytehouse://gateway.staging.bytehouse.cloud:19000");
        final Properties properties = new Properties();
        properties.setProperty("account_id", "GCP02U60");
        properties.setProperty("user", "zx");
        properties.setProperty("password", "P@55word");
        properties.setProperty("secure", "true");

        final DataSource dataSource = new BalancedByteHouseDataSource(url, properties);
        try (Connection connection = dataSource.getConnection()) {
            createDatabase(connection);
            createTable(connection);
            insertTable(connection);
            selectTable(connection);
            dropDatabase(connection);
        }
    }

    public static void createDatabase(Connection connection) {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("CREATE DATABASE IF NOT EXISTS inventory");
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    public static void createTable(Connection connection) {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(
                    "CREATE TABLE IF NOT EXISTS inventory.orders\n" +
                            "(" +
                            "    OrderID String," +
                            "    OrderName String," +
                            "    OrderPriority Int8" +
                            ")" +
                            "    engine = CnchMergeTree()" +
                            "    partition by OrderID" +
                            "    order by OrderID"
            );
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    public static void insertTable(Connection connection) {
        try (Statement stmt = connection.createStatement()) {
            stmt.executeUpdate(
                    "INSERT INTO inventory.orders VALUES ('54895','Apple',12)"
            );
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    public static void selectTable(Connection connection) {
        try (Statement stmt = connection.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT * FROM inventory.orders");
            ResultSetMetaData rsmd = rs.getMetaData();
            int columnsNumber = rsmd.getColumnCount();
            while (rs.next()) {
                for (int i = 1; i <= columnsNumber; i++) {
                    if (i > 1) System.out.print(", ");
                    String columnValue = rs.getString(i);
                    System.out.print(columnValue);
                }
                System.out.println();
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    public static void dropDatabase(Connection connection) {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP DATABASE inventory");
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }
}
