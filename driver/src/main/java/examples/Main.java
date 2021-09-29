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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

@SuppressWarnings("PMD.SystemPrintln")
public class Main {

    public static void main(String[] args) throws Exception {
        String url = "jdbc:bytehouse://localhost:9000";
        Properties properties = new Properties();
        properties.setProperty("account", "id");
        properties.setProperty("user", "test");
        properties.setProperty("password", "password");

        // Registers the ByteHouse JDBC driver with DriverManager
        Class.forName("com.bytedance.bytehouse.jdbc.ByteHouseDriver");

        // Obtain Connection with DriverManager
        try (Connection connection = DriverManager.getConnection(url, properties)) {
            try (Statement stmt = connection.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("SELECT 5")) {
                    while (rs.next()) {
                        System.out.println(rs.getInt(1));
                    }
                }
            }
        }
    }
}
