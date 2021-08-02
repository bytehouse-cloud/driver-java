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

import javax.sql.DataSource;
import java.sql.*;
import java.util.Properties;

public class SimpleQuery {

    public static void main(String[] args) throws Exception {
        // Following environment variables must be defined
        String url = String.format("jdbc:bytehouse://localhost:9000");
        Properties properties = new Properties();
        properties.setProperty("account_id", "");
        properties.setProperty("user", "default");
        properties.setProperty("password", "");

        DataSource dataSource = new BalancedByteHouseDataSource(url, properties);
        Connection connection = dataSource.getConnection();

        Statement stmt = connection.createStatement();

        stmt.executeQuery(
                "use qifeng"
        );

//        stmt.executeQuery(
//                "create TABLE arraytest1 (i Array(Int8)) ENGINE = MergeTree ORDER BY i"
//        );

        stmt.executeUpdate(
                "insert into lowcardinalitytest values ('12th'),('13th'),('14th')"
        );

        ResultSet rs = stmt.executeQuery(
                "select * from lowcardinalitytest"
        );

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

        connection.close();
    }
}
