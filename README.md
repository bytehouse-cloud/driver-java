
# ByteHouse JDBC Driver

The ByteHouse JDBC driver allows programs written in JVM languages (Java, Kotlin, Scala) to connect to ByteHouse.
The driver is a Type 4 JDBC driver written purely in Java, and is thus platform independent, and it communicates 
with ByteHouse using its native network protocol. The driver implements the 
<a href="https://docs.oracle.com/javase/8/docs/api/java/sql/package-summary.html">JDBC API</a>.

## Getting Started

### Requirements
- Java 8 or higher

### Installation
Simply add the compiled jar file into your project, or use your chosen dependency 
management tool to add this project as a dependency. You can then import and use the classes
in your Java program.

#### Adding Driver as a Gradle Dependency
```
implementation "com.bytedance.bytehouse:driver-java:1.0.0"
```

#### Adding Driver as a Maven Dependency
```
<dependency>
  <groupId>com.bytedance.bytehouse</groupId>
  <artifactId>driver-java</artifactId>
  <version>1.0.0</version>
</dependency>
```

## Usage

### Connect to ByteHouse

To connect to the ByteHouse, you need to specify the ByteHouse URL with your account and user information. You
can visit [ByteHouse China](bytehouse.cn) (for China-mainland) or [Bytehouse Global](bytehouse.cloud) (for
non-China-mainland) to register account.

The below login parameters is the same as if you were to login using the web console:
- Account Name
- Region
- User Name
- Password

### JDBC URL format
The following is the accepted URL format for [ByteHouse China](bytehouse.cn) & [Bytehouse Global](bytehouse.cloud)
```
"jdbc:bytehouse:///?region=CN-NORTH-1&account=YOUR_ACCOUNT_NAME&user=YOUR_USER&password=YOUR_PASSWORD"
"jdbc:bytehouse:///?region=AP-SOUTHEAST-1&account=YOUR_ACCOUNT_NAME&user=YOUR_USER&password=YOUR_PASSWORD"
```

Your Java application can connect to ByteHouse using either one of two classes, as documented in the
<a href="https://docs.oracle.com/javase/tutorial/jdbc/basics/connecting.html">JDBC docs</a>.
They are the DriverManager and DataSource.

### JDBC API
Implements           | Class
---                  | ---
java.sql.Driver      | com.bytedance.bytehouse.jdbc.ByteHouseDriver
javax.sql.DataSource | com.bytedance.bytehouse.jdbc.ByteHouseDataSource

### Connecting using ByteHouseDriver 

```
String url = "jdbc:bytehouse:///?region=CN-NORTH-1";

Properties properties = new Properties();
properties.setProperty("account", "YOUR_ACCOUNT_NAME");
properties.setProperty("user", "YOUR_USER_NAME");
properties.setProperty("password", "YOUR_PASSWORD");

Connection connection = new ByteHouseDriver().connect(url, properties);
```

### Connecting using ByteHouseDataSource

```
String url = "jdbc:bytehouse:///?region=CN-NORTH-1";

Properties properties = new Properties();
properties.setProperty("account", "YOUR_ACCOUNT_NAME");
properties.setProperty("user", "YOUR_USER_NAME");
properties.setProperty("password", "YOUR_PASSWORD");

Connection connection = new ByteHouseDataSource(url, properties).getConnection();
```

### Performing DDL Query

```
try (Statement stmt = connection.createStatement()) {
    String createDatabaseSql = "CREATE DATABASE IF NOT EXISTS inventory";
    stmt.execute(createDatabaseSql);
    
    String createTableSql = "CREATE TABLE IF NOT EXISTS inventory.orders (" +
                    " OrderID String, " +
                    " OrderName String, " +
                    " OrderPriority Int8 " +
                    " ) " +
                    " engine = CnchMergeTree()" +
                    " partition by OrderID" +
                    " order by OrderID";
    stmt.execute(createTableSql);                 
} catch (SQLException ex) {
    ex.printStackTrace();
}
```

### Performing DML Query

```
try (Statement stmt = connection.createStatement()) {
    String insertSql = "INSERT INTO inventory.orders VALUES('54895','Apple',12)";
    stmt.executeUpdate(insertSql);                 
} catch (SQLException ex) {
    ex.printStackTrace();
}
```

```
String batchInsertSql = "INSERT INTO inventory.orders VALUES(?,'Apple',?)";
try (PreparedStatement pstmt = connection.prepareStatement(batchInsertSql)) {
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
```

### Performing DQL Query

```
try (Statement stmt = connection.createStatement()) {
    String selectSql = "SELECT * FROM inventory.orders";
    ResultSet rs = stmt.execute(selectSql);              
} catch (SQLException ex) {
    ex.printStackTrace();
}
```

## License

This project is distributed under the terms of the Apache License (Version 2.0). See [LICENSE](LICENSE) for details.
