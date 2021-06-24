package examples;

import com.bytedance.bytehouse.jdbc.BalancedClickhouseDataSource;

import javax.sql.DataSource;
import java.sql.*;
import java.util.Properties;

public class SimpleQuery {

    public static void main(String[] args) throws Exception {
        // Following environment variables must be defined
        String url = String.format("jdbc:clickhouse://%s:%s", System.getenv("HOST"), System.getenv("PORT"));
        Properties properties = new Properties();
        properties.setProperty("account_id", System.getenv("ACCOUNT_ID"));
        properties.setProperty("user", System.getenv("USER"));
        properties.setProperty("password", System.getenv("PASSWORD"));

        DataSource dataSource = new BalancedClickhouseDataSource(url, properties);
        Connection connection = dataSource.getConnection();

        Statement stmt = connection.createStatement();

        ResultSet rs = stmt.executeQuery(
                "SELECT (number % 3 + 1) as n, sum(number) FROM numbers(10000000) GROUP BY n"
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
