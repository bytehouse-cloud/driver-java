
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
```java
import com.bytedance.bytehouse.jdbc.BalancedByteHouseDataSource;

import javax.sql.DataSource;
import java.sql.*;
import java.util.Properties;

public class Main {

    public static void main(String[] args) throws Exception {
        String url = "jdbc:bytehouse://localhost:9000";
        Properties properties = new Properties();
        properties.setProperty("account_id", "id");
        properties.setProperty("user", "test");
        properties.setProperty("password", "password");

        // Obtain DataSource with url and properties set
        DataSource dataSource = new BalancedByteHouseDataSource(url, properties);

        // Obtain Connection with DataSource
        try (Connection connection = dataSource.getConnection()) {
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

### Connecting using the DriverManager
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
