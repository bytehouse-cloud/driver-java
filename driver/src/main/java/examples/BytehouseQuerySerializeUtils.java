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

import com.bytedance.bytehouse.jdbc.ByteHouseDriver;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.List;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.ArrayList;

public class BytehouseQuerySerializeUtils {

    public static void main(String[] args) throws Exception {

        final Properties properties = new Properties();

        final String url = "jdbc:bytehouse:///?region=BOE";
        properties.setProperty("access_key", "");
        properties.setProperty("secret_key", "");
        properties.setProperty("is_volcano", "true");
        properties.setProperty("secure", "true");
        properties.setProperty("vw", "test");

        final ByteHouseDriver driver = new ByteHouseDriver();
        Connection connection = driver.connect(url, properties);

        createDatabase(connection);
        createTable(connection);
        insertTable(connection);
        getAndSerialize(connection);
        dropDatabase(connection);
    }

    public static void createDatabase(Connection connection) {
        final String sql = "CREATE DATABASE IF NOT EXISTS test_hieu";
        System.out.println(sql);
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(sql);
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    public static void dropDatabase(Connection connection) {
        final String sql = "DROP DATABASE test_hieu";
        System.out.println(sql);
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(sql);
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    public static void createTable(Connection connection) {
        try (Statement stmt = connection.createStatement()) {
            final String sql = "CREATE TABLE IF NOT EXISTS test_hieu.orders_test (" +
                    " Arr8 Array(String), " +
                    " Arr9 Array(FixedString(10)), " +
                    " OrderID FixedString(10), " +
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

    public static void insertTable(Connection connection) {
        try (Statement stmt = connection.createStatement()) {
            final String sql = "INSERT INTO test_hieu.orders_test "
                    + " (Arr8, Arr9, OrderID, OrderName, OrderPriority) "
                    + " VALUES "
                    + " (['1', '2'], ['3', '4'], '1', 'Apple', 1) ";
            System.out.println(sql);
            stmt.executeUpdate(sql);
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    public static void getAndSerialize(Connection connection) {
        final String sql = "SELECT * FROM test_hieu.orders_test";
        System.out.println(sql);
        try (Statement stmt = connection.createStatement()) {
            ResultSet rs = stmt.executeQuery(sql);
            List<Map<String, Object>> rows = getResult(rs);
            System.out.println(rows);
            RawResult rawResult = new RawResult();
            rawResult.rows = rows;
            byte[] serializedBytes = serialize(rawResult);
            System.out.println(serializedBytes);
            System.out.println(deserialize(serializedBytes).rows);
        } catch (SQLException ex) {
            ex.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static List<Map<String, Object>> getResult(ResultSet resultSet) throws SQLException {
        List<Map<String, Object>> result = new ArrayList();
        if (resultSet == null) {
            return result;
        } else {
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();
            String[] columnNames = new String[columnCount];

            for (int i = 0; i < columnCount; ++i) {
                columnNames[i] = metaData.getColumnName(i + 1);
            }

            while (resultSet.next()) {
                Map<String, Object> map = new LinkedHashMap();

                for (int i = 0; i < columnCount; ++i) {
                    map.put(columnNames[i], resultSet.getObject(i + 1));
                }

                result.add(map);
            }

            return result;
        }
    }

    public static byte[] serialize(RawResult rawResult) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream out = null;
        try {
            ObjectOutputStream objectOutputStream
                    = new ObjectOutputStream(bos);
            objectOutputStream.writeObject(rawResult);
            objectOutputStream.flush();
            byte[] yourBytes = bos.toByteArray();
            return yourBytes;
        } finally {
            try {
                if (out != null) {
                    out.close();
                }
                bos.close();
            } catch (IOException ex) {
                // ignore close exception
            }
        }
    }

    public static RawResult deserialize(byte[] serializedBytes) throws IOException {
        ByteArrayInputStream bis = null;
        ObjectInput in = null;
        try {
            bis = new ByteArrayInputStream(serializedBytes);
            in = new ObjectInputStream(bis);
            return (RawResult) in.readObject();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            try {
                if (in != null) {
                    in.close();
                }
                bis.close();
            } catch (IOException ex) {
                // ignore close exception
            }
        }
        return null;
    }
}
