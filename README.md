
# ByteHouse JDBC Driver

The ByteHouse JDBC driver allows programs written in JVM languages (Java, Kotlin, Scala) to connect to ByteHouse.
The driver is a Type 4 JDBC driver written purely in Java, and is thus platform independent, and it communicates 
with ByteHouse using its native network protocol. The driver implements the 
<a href="https://docs.oracle.com/javase/8/docs/api/java/sql/package-summary.html">JDBC API</a>.

This project aims to provide official support for Java database connectivity to Bytehouse, to ensure full compatibility
with ByteHouse. This project is adapted from the open-source
<a href="https://github.com/housepower/ClickHouse-Native-JDBC">ClickHouse Native JDBC driver project</a>.

For more information, refer to the official 
<a href="https://bytedance.feishu.cn/wiki/wikcns7hYkiy8nqxxwN2X6LXfFh">ByteHouse JDBC Driver documentation</a>.

## Features

- Data sent to/received from ByteHouse is compressed by default.
- Implemented in the TCP protocol, and is arguably more performant than in HTTP.
- Supports secure connection establishment with ByteHouse using TLS.
- Supports ByteHouse specific query settings.

## Getting Started

### Requirements
- Java 8 or higher

### Installation
Simply add the compiled jar file into your project, or use your chosen dependency 
management tool to add this project as a dependency. You can then import and use the classes
in your Java program.

#### Adding Driver as a Gradle Dependency
```
implementation "com.bytedance.bytehouse:driver-java:0.1.0-SNAPSHOT"
```

#### Adding Driver as a Maven Dependency
```
<dependency>
  <groupId>com.bytedance.bytehouse</groupId>
  <artifactId>driver-java</artifactId>
  <version>0.1.0-SNAPSHOT</version>
</dependency>
```

## Usage

Your Java application can connect to ByteHouse using either one of two classes, as documented in the 
<a href="https://docs.oracle.com/javase/tutorial/jdbc/basics/connecting.html">JDBC docs</a>.
They are the DriverManager and DataSource.

### JDBC API
Implements           | Class
---                  | ---
java.sql.Driver      | com.bytedance.bytehouse.jdbc.ByteHouseDriver
javax.sql.DataSource | com.bytedance.bytehouse.jdbc.BalancedByteHouseDataSource 

### Connecting using the DataSource (Recommended)
```bash
# how to run
./gradlew -PmainClass=examples.SimpleQuery run
```
```java
import com.bytedance.bytehouse.jdbc.BalancedByteHouseDataSource;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import javax.sql.DataSource;

public class SimpleQuery {
    public static void main(String[] args) throws Exception {
        String url = String.format("jdbc:bytehouse://gateway.aws-cn-north-1.bytehouse.cn:19000");
        Properties properties = new Properties();
        properties.setProperty("account_id", "AWS12345");
        properties.setProperty("user", "username");
        properties.setProperty("password", "YOUR_PASSWORD");
        properties.setProperty("secure", "true");

        DataSource dataSource = new BalancedByteHouseDataSource(url, properties);

        try (Connection connection = dataSource.getConnection()) {
            createDatabase(connection);
            createTable(connection);
            insertTable(connection);
            insertBatch(connection);
            selectTable(connection);
            dropDatabase(connection);

        } catch (SQLException ex) {
            ex.printStackTrace();
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

    public static void insertBatch(Connection connection) {
        String insertQuery = "INSERT INTO inventory.orders (OrderID, OrderName, OrderPriority) VALUES (?,'Apple',?)";
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
```

### Connecting using the DriverManager
```bash
# how to run
./gradlew -PmainClass=examples.Main run
```
```java
import java.sql.*;
import java.util.Properties;

public class Main {

    public static void main(String[] args) throws Exception {
        String url = "jdbc:bytehouse://localhost:9000";
        Properties properties = new Properties();
        properties.setProperty("account_id", "id");
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
```

### JDBC URL format
The following is the accepted URL format. Arguments in brackets [] are optional.
```
jdbc:bytehouse://host:port/[database]
    [?propertyName1][=propertyValue1]
    [&propertyName2][=propertyValue2]...
```

The driver only recognises URL with the correct sub-protocol 'jdbc:bytehouse'.
Refer to <a href="https://bytedance.feishu.cn/wiki/wikcns7hYkiy8nqxxwN2X6LXfFh">ByteHouse JDBC Driver documentation</a>
for the list of accepted properties.

## License

This project is distributed under the terms of the Apache License (Version 2.0). See [LICENSE](LICENSE) for details.
