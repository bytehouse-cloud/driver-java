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
