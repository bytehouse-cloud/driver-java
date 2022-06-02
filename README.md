# ByteHouse JDBC Driver
## Introduction
ByteHouse provides a JDBC type 4 driver that supports core JDBC functionality. The JDBC driver must be installed in a 
64-bit environment and requires Java 1.8. The driver can be used with most client tools/applications that support JDBC 
for connecting to ByteHouse datasource.
## Table of Contents
- [Requirements](#requirements)
- [Installation](#installation)
    * [Adding Driver as a Gradle Dependency](#adding-driver-as-a-gradle-dependency)
    * [Adding Driver as a Maven Dependency](#adding-driver-as-a-maven-dependency)
    * [Direct Download](#direct-download)
- [Usage](#usage)
    * [Creating ByteHouse Account](#creating-bytehouse-account)
    * [JDBC Connection URL Format](#jdbc-connection-url-format)
        + [URL Format for password authentication](#url-format-for-password-authentication)
        + [URL Format for AK/SK authentication](#url-format-for-ak-sk-authentication)
        + [URL Format for IP & Port](#url-format-for-ip---port)
        + [Adding additional parameters for URL](#adding-additional-parameters-for-url)
    * [JDBC API](#jdbc-api)
        + [Connecting using ByteHouseDriver](#connecting-using-bytehousedriver)
        + [Connecting using ByteHouseDataSource](#connecting-using-bytehousedatasource)
    * [Setting Virtual WareHouse](#setting-virtual-warehouse)
    * [Setting Role](#setting-role)
    * [Performing SQL Queries](#performing-sql-queries)
        + [DDL Query](#ddl-query)
        + [DML Query](#dml-query)
            - [Row Insert](#row-insert)
            - [Batch Insert](#batch-insert)
        + [DQL Query](#dql-query)
- [Supported DataTypes](#supported-datatypes)
    * [ByteHouse Definition](#bytehouse-definition)
    * [Driver Definition](#driver-definition)
- [Usage of DataTypes](#usage-of-datatypes)
    * [DataTypeUInt8](#datatypeuint8)
    * [DataTypeUInt16](#datatypeuint16)
    * [DataTypeUInt32](#datatypeuint32)
    * [DataTypeUInt64](#datatypeuint64)
    * [DataTypeInt8](#datatypeint8)
    * [DataTypeInt16](#datatypeint16)
    * [DataTypeInt32](#datatypeint32)
    * [DataTypeInt64](#datatypeint64)
    * [DataTypeFloat32](#datatypefloat32)
    * [DataTypeFloat64](#datatypefloat64)
    * [DataTypeDecimal](#datatypedecimal)
    * [DataTypeString](#datatypestring)
    * [DataTypeFixedString](#datatypefixedstring)
    * [DataTypeIPv4](#datatypeipv4)
    * [DataTypeIPv6](#datatypeipv6)
    * [DataTypeUUID](#datatypeuuid)
    * [DataTypeDate](#datatypedate)
    * [DataTypeDateTime](#datatypedatetime)
    * [DataTypeEnum8](#datatypeenum8)
    * [DataTypeEnum16](#datatypeenum16)
    * [DataTypeNullable](#datatypenullable)
    * [DataTypeArray](#datatypearray)
        + [ByteHouse Array](#bytehouse-array)
    * [DataTypeMap](#datatypemap)
    * [DataTypeTuple](#datatypetuple)
        + [ByteHouseStruct](#bytehousestruct)
- [Integration with BI Tools](#integration-with-bi-tools)
    * [Tableau Integration](#tableau-integration)
    * [DataGrip Integration](#datagrip-integration)
    * [DBeaver Integration](#dbeaver-integration)
- [Parameters Reference](#parameters-reference)
    * [Authentication Parameters](#authentication-parameters)
    * [Connection Parameters](#connection-parameters)
    * [Query / Server Side Parameters](#query---server-side-parameters)
- [Troubleshooting](#troubleshooting)
- [Issue Reporting](#issue-reporting)
- [License](#license)
## Requirements
* Java 1.8 or higher
## Installation
### Adding Driver as a Gradle Dependency
```
    repositories {
        // This is public bytedance repository for downloading artifacts
        maven {
            url "https://artifact.bytedance.com/repository/releases"
        }
    }
    
    dependencies {
        implementation 'com.bytedance.bytehouse:driver-java:1.0.0'
    }
```
### Adding Driver as a Maven Dependency
```
        // This is public bytedance repository for downloading artifacts
        <repository>
            <id>bytedance</id>
            <name>ByteDance Public Repository</name>
            <url>https://artifact.bytedance.com/repository/releases</url>
        </repository>
        
        <dependency>
            <groupId>com.bytedance.bytehouse</groupId>
            <artifactId>driver-java</artifactId>
            <version>1.0.0</version>
        </dependency>
```
### Direct Download
* Go to the ByteDance Maven Repository: 
https://artifact.bytedance.com/repository/releases/com/bytedance/bytehouse/driver-java/
* Click on the directory for the version that you need. The most recent version is not always at the end of the list.
* Download the driver-java-#.#.#-all.jar file.
## Usage
### Creating ByteHouse Account
You need to create ByteHouse account in order to use JDBC Driver. You can simply create a free account with the process 
mentioned in our official website documentation: https://docs.bytehouse.cloud/en/docs/quick-start
<br/><br/>
You can also create ByteHouse account through Volcano Engine by ByteDance: 
https://www.volcengine.com/product/bytehouse-cloud
### JDBC Connection URL Format
#### URL Format for password authentication
For password authentication, Region, Account ID, User ID & Password parameters are required. 
```java
    String connectionURL = String.format("jdbc:bytehouse:///?region=%s&account=%s&user=%s&password=%s", REGION, ACCOUNT, USER, PASSWORD);
```
#### URL Format for AK/SK authentication
For AK/SK authentication, Region, Access Key & Secret Key parameters are required. 
```java
    String connectionURL = String.format("jdbc:bytehouse:///?region=%s&access_key=%s&secret_key=%s&is_volcano=true", REGION, ACCESS_KEY, SECRET_KEY);
```
#### URL Format for IP & Port
You can directly use Host Name / IP & Port addresses instead of region.
```java
    String connectionURL = String.format("jdbc:bytehouse://%s:%s/?account=%s&user=%s&password=%s", HOST, PORT, ACCOUNT, USER, PASSWORD);
```

```java
    String connectionURL = String.format("jdbc:bytehouse://%s:%s/?access_key=%s&secret_key=%s&is_volcano=true", HOST, PORT, ACCESS_KEY, SECRET_KEY);
```
#### Adding additional parameters for URL
You can create <a href="https://docs.oracle.com/javase/8/docs/api/java/util/Properties.html">Properties</a> object to 
supply additional parameters alongside with JDBC connection URL. References of additional parameters can be found
[here](#parameters-reference).
```java
    Properties properties = new Properies();
    properties.setProperty("secure", "false");
```
### JDBC API
Implements           | Class
---                  | ---
java.sql.Driver      | com.bytedance.bytehouse.jdbc.ByteHouseDriver
javax.sql.DataSource | com.bytedance.bytehouse.jdbc.ByteHouseDataSource
#### Connecting using ByteHouseDriver
```java
    Connection connection = new ByteHouseDriver().connect(connectionURL, properties);
```
#### Connecting using ByteHouseDataSource
```java
    Connection connection = new ByteHouseDataSource(connectionURL, properties).getConnection();
```
### Setting Virtual WareHouse
A virtual warehouse is a cluster of computing resources in Bytehouse that we can scale out on demand. A warehouse 
provides the required resources, such as CPU, memory, and temporary storage, to perform the database operations.

User can set the virtual warehouse using ByteHouseStatement object. After setting the virtual warehouse, user can obtain
the connection object from ByteHouseStatement object and can subsequently use the same connection object to perform 
database operations with the same virtual warehouse.

If users wants to change virtual warehouse, they have to create new connection object with the above mentioned 
procedure. In case, there is no virtual warehouse stated, the server may use the default virtual warehouse, if there is,
or can throw exception message. An example case of setting virtual warehouse is shown below:
```java
    try (Statement stmt = connection.createStatement()) {
        stmt.execute("SET WAREHOUSE vw_name");
    } catch (SQLException ex) {
        ex.printStackTrace();
    }
```
### Setting Role
When using ByteHouse, you need to select an "Active Role", and all your behaviour will be restricted by the permissions
assigned to this Active Role.

User can set the active role using ByteHouseStatement object. After setting the active role, user can obtain
the connection object from ByteHouseStatement object and can subsequently use the same connection object to perform
database operations with the same active role.
```java
    try (Statement stmt = connection.createStatement()) {
        stmt.execute("SET ROLE role_name");
    } catch (SQLException ex) {
        ex.printStackTrace();
    }
```
### Performing SQL Queries
#### DDL Query
```java
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
#### DML Query
##### Row Insert
```java
    try (Statement stmt = connection.createStatement()) {
        String insertSql = "INSERT INTO inventory.orders VALUES('54895','Apple',12)";
        stmt.executeUpdate(insertSql);                 
    } catch (SQLException ex) {
        ex.printStackTrace();
    }
```
##### Batch Insert
```java
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
#### DQL Query
```java
    try (Statement stmt = connection.createStatement()) {
        String selectSql = "SELECT * FROM inventory.orders";
        ResultSet rs = stmt.executeQuery(selectSql);              
    } catch (SQLException ex) {
        ex.printStackTrace();
    }
```
## Supported DataTypes 
### ByteHouse Definition
Definition of datatypes in ByteHouse datasource can be found here: https://docs.bytehouse.cloud/en/docs/data-types
### Driver Definition
- Class - Class represents the top Java class name for each of the datatypes that are available in ByteHouse.
- Driver Data Type - Represents the Java class for each of the ByteHouse data types. You can insert & select different 
datatypes using these classes.
- JDBC Data Type - Represents the internal JDBC driver class. Driver data type is converted to JDBC data type before 
sending them to server.
- Types - Representation of java.sql.Types for each of the ByteHouse datatypes.
<table><tr><td class="selected" style="text-align: center; vertical-align: top;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>No</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>Class (ByteHouse Type)</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>Driver Data Type</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>JDBC Data Type</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>Types</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>Precision</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>Scale</span></p></div></div></td></tr><tr><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>1</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>DataTypeUInt8</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>Short</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>Short</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>Types.TINYINT</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>3</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>0</span></p></div></div></td></tr><tr><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>2</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>DataTypeUInt16</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>Integer</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>Integer</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>Types.SMALLINT</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>5</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>0</span></p></div></div></td></tr><tr><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>3</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>DataTypeUInt32</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>Long</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>Long</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>Types.INTEGER</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>10</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>0</span></p></div></div></td></tr><tr><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>4</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>DataTypeUInt64</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>BigInteger</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>BigInteger</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>Types.BIGINT</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>19</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>0</span></p></div></div></td></tr><tr><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>5</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>DataTypeInt8</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>Byte</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>Byte</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>Types.TINYINT</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>4</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>0</span></p></div></div></td></tr><tr><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>6</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>DataTypeInt16</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>Short</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>Short</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>Types.SMALLINT</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>6</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>0</span></p></div></div></td></tr><tr><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>7</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>DataTypeInt32</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>Integer</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>Integer</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>Types.INTEGER</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>11</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>0</span></p></div></div></td></tr><tr><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>8</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>DataTypeInt64</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>Long</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>Long</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>Types.BIGINT</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>20</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>0</span></p></div></div></td></tr><tr><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>9</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>DataTypeFloat32</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>Float</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>Float</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>Types.FLOAT</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>8</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>8</span></p></div></div></td></tr><tr><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>10</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>DataTypeFloat64</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>Double</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>Double</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>Types.DOUBLE</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>17</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>17</span></p></div></div></td></tr><tr><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>11</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>DataTypeDecimal</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>BigDecimal</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>BigDecimal</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>Types.DECIMAL</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>User defined</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>User defined</span></p></div></div></td></tr><tr><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>12</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>DataTypeString</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>CharSequence</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>String</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>Types.VARCHAR</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>0</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>0</span></p></div></div></td></tr><tr><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>13</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>DataTypeFixedString</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>CharSequence</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>String</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>Types.VARCHAR</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>User defined</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>0</span></p></div></div></td></tr><tr><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>14</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>DataTypeIPv4</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>Long</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>Long</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>Types.INTEGER</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>0</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>15</span></p></div></div></td></tr><tr><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>15</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>DataTypeIPv6</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>Inet6Address</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>String</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>Types.VARCHAR</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>0</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>0</span></p></div></div></td></tr><tr><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>16</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>DataTypeUUID</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>UUID</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>String</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>Types.VARCHAR</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>36</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>0</span></p></div></div></td></tr><tr><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>17</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>DataTypeDate</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>LocalDate</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>Date</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>Types.DATE</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>10</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>0</span></p></div></div></td></tr><tr><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>18</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>DataTypeDateTime</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>ZonedDateTime</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>Timestamp</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>Types.TIMESTAMP</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>0</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>10</span></p></div></div></td></tr><tr><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>19</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>DataTypeEnum8</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>String</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>String</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>Types.VARCHAR</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>0</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>0</span></p></div></div></td></tr><tr><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>20</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>DataTypeEnum16</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>String</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>String</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>Types.VARCHAR</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>0</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>0</span></p></div></div></td></tr><tr><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>21</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>DataTypeNullable</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>Nested Type</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>Nested Type</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>Nested Type</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>0</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>0</span></p></div></div></td></tr><tr><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>22</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>DataTypeArray</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>ByteHouseArray</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>Array</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>Types.ARRAY</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>0</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>0</span></p></div></div></td></tr><tr><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>23</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>DataTypeMap</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>Map</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>Object</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>Types.OTHER</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>0</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>0</span></p></div></div></td></tr><tr><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>24</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>DataTypeTuple</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>ByteHouseStruct</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>Struct</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>Types.STRUCT</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>0</span></p></div></div></td><td class="selected" style="vertical-align: top; text-align: center;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>0</span></p></div></div></td></tr></table>

## Usage of DataTypes
### DataTypeUInt8
```java
    // pstmt is the object for PreparedStatement class
    
    Short valueShort = 1;
    short valueShortPrimitive = 1;
    
    pstmt.setShort(1, valueShort);
    pstmt.addBatch();
    
    pstmt.setShort(1, valueShortPrimitive);
    pstmt.addBatch();
    
    pstmt.setObject(1, valueShort);
    pstmt.addBatch();
    
    pstmt.setObject(1, valueShortPrimitive);
    pstmt.addBatch();
    
    pstmt.executeBatch();
```
### DataTypeUInt16
```java
    // pstmt is the object for PreparedStatement class
    
    Integer valueInteger = 35000;
    int valueIntegerPrimitive = 35000;
    
    pstmt.setInt(1, valueInteger);
    pstmt.addBatch();
    
    pstmt.setInt(1, valueIntegerPrimitive);
    pstmt.addBatch();
    
    pstmt.setObject(1, valueInteger);
    pstmt.addBatch();
    
    pstmt.setObject(1, valueIntegerPrimitive);
    pstmt.addBatch();
    
    pstmt.executeBatch();
```
### DataTypeUInt32
```java
    // pstmt is the object for PreparedStatement class
    
    Long valueLong = Long.valueOf(256256256256L);
    long valueLongPrimitive = 256256256256L;
    
    pstmt.setLong(1, valueLong);
    pstmt.addBatch();
    
    pstmt.setLong(1, valueLongPrimitive);
    pstmt.addBatch();
    
    pstmt.setObject(1, valueLong);
    pstmt.addBatch();
    
    pstmt.setObject(1, valueLongPrimitive);
    pstmt.addBatch();
    
    pstmt.executeBatch();
```
### DataTypeUInt64
```java
    // pstmt is the object for PreparedStatement class
    
    BigInteger bigIntegerValue = BigInteger.valueOf(256256256256L);
    
    pstmt.setObject(1, bigIntegerValue);
    pstmt.addBatch();
    
    pstmt.executeBatch();
```
### DataTypeInt8
```java
    // pstmt is the object for PreparedStatement class
    
    Byte valueByte = 1;
    byte valueBytePrimitive = 1;
    
    pstmt.setByte(1, valueByte);
    pstmt.addBatch();
    
    pstmt.setByte(1, valueBytePrimitive);
    pstmt.addBatch();
    
    pstmt.setObject(1, valueByte);
    pstmt.addBatch();
    
    pstmt.setObject(1, valueBytePrimitive);
    pstmt.addBatch();
    
    pstmt.executeBatch();
```
### DataTypeInt16
```java
    // pstmt is the object for PreparedStatement class
    
    Short valueShort = 1;
    short valueShortPrimitive = 1;
    
    pstmt.setShort(1, valueShort);
    pstmt.addBatch();
    
    pstmt.setShort(1, valueShortPrimitive);
    pstmt.addBatch();
    
    pstmt.setObject(1, valueShort);
    pstmt.addBatch();
    
    pstmt.setObject(1, valueShortPrimitive);
    pstmt.addBatch();
    
    pstmt.executeBatch();
```
### DataTypeInt32
```java
    // pstmt is the object for PreparedStatement class
    
    Integer valueInteger = 35000;
    int valueIntegerPrimitive = 35000;
    
    pstmt.setInt(1, valueInteger);
    pstmt.addBatch();
    
    pstmt.setInt(1, valueIntegerPrimitive);
    pstmt.addBatch();
    
    pstmt.setObject(1, valueInteger);
    pstmt.addBatch();
    
    pstmt.setObject(1, valueIntegerPrimitive);
    pstmt.addBatch();
    
    pstmt.executeBatch();
```
### DataTypeInt64
```java
    // pstmt is the object for PreparedStatement class
    
    Long valueLong = Long.valueOf(256256256256L);
    long valueLongPrimitive = 256256256256L;
    
    pstmt.setLong(1, valueLong);
    pstmt.addBatch();
    
    pstmt.setLong(1, valueLongPrimitive);
    pstmt.addBatch();
    
    pstmt.setObject(1, valueLong);
    pstmt.addBatch();
    
    pstmt.setObject(1, valueLongPrimitive);
    pstmt.addBatch();
    
    pstmt.executeBatch();
```
### DataTypeFloat32
```java
    // pstmt is the object for PreparedStatement class
    
    Float valueFloat = Float.valueOf("123.45879");
    float valueFloatPrimitive = 123.45897f;
    
    pstmt.setFloat(1, valueFloat);
    pstmt.addBatch();
    
    pstmt.setFloat(1, valueFloatPrimitive);
    pstmt.addBatch();
    
    pstmt.setObject(1, valueFloat);
    pstmt.addBatch();
    
    pstmt.setObject(1, valueFloatPrimitive);
    pstmt.addBatch();
    
    pstmt.executeBatch();
```
### DataTypeFloat64
```java
    // pstmt is the object for PreparedStatement class
    
    Double valueDouble = Double.valueOf("123.45879");
    double valueDoublePrimitive = 123.45897;
    
    pstmt.setDouble(1, valueDouble);
    pstmt.addBatch();
    
    pstmt.setDouble(1, valueDoublePrimitive);
    pstmt.addBatch();
    
    pstmt.setObject(1, valueDouble);
    pstmt.addBatch();
    
    pstmt.setObject(1, valueDoublePrimitive);
    pstmt.addBatch();
    
    pstmt.executeBatch();
```
### DataTypeDecimal
```java
    // pstmt is the object for PreparedStatement class
    
    BigDecimal valueBigDecimal = BigDecimal.valueOf(123.456);
    
    pstmt.setBigDecimal(1, valueBigDecimal);
    pstmt.addBatch();
    
    pstmt.setObject(1, valueBigDecimal);
    pstmt.addBatch();
    
    pstmt.executeBatch();
```
### DataTypeString
```java
    // pstmt is the object for PreparedStatement class
    
    String valueString = "tobeInserted";
    CharSequence valueCharSequence = "charSequence";
    
    pstmt.setString(1, valueString);
    pstmt.addBatch();
    
    pstmt.setString(1, valueCharSequence.toString());
    pstmt.addBatch();
    
    pstmt.setObject(1, valueString);
    pstmt.addBatch();
    
    pstmt.setObject(1, valueCharSequence);
    pstmt.addBatch();
    
    pstmt.executeBatch();
```
### DataTypeFixedString
```java
    // pstmt is the object for PreparedStatement class
    
    String valueString = "abcdefghij";
    CharSequence valueCharSequence = "abcdefghij";
    
    pstmt.setString(1, valueString);
    pstmt.addBatch();
    
    pstmt.setString(1, valueCharSequence.toString());
    pstmt.addBatch();
    
    pstmt.setObject(1, valueString);
    pstmt.addBatch();
    
    pstmt.setObject(1, valueCharSequence);
    pstmt.addBatch();
    
    pstmt.executeBatch();
```
### DataTypeIPv4
```java
    // pstmt is the object for PreparedStatement class
    
    Long valueLong = Long.valueOf(256256256256L);
    long valueLongPrimitive = 256256256256L;
    
    pstmt.setLong(1, valueLong);
    pstmt.addBatch();
    
    pstmt.setLong(1, valueLongPrimitive);
    pstmt.addBatch();
    
    pstmt.setObject(1, valueLong);
    pstmt.addBatch();
    
    pstmt.setObject(1, valueLongPrimitive);
    pstmt.addBatch();
    
    pstmt.executeBatch();
```
### DataTypeIPv6
```java
    // pstmt is the object for PreparedStatement class
    
    String valueString = "2001:44c8:129:2632:33:0:252:2";
    Inet6Address valueInet6Address = (Inet6Address) Inet6Address.getByName("2001:44c8:129:2632:33:0:252:2");
    
    pstmt.setString(1, valueString);
    pstmt.addBatch();
    
    pstmt.setString(1, valueInet6Address.getHostAddress());
    pstmt.addBatch();
    
    pstmt.setObject(1, valueString);
    pstmt.addBatch();
    
    pstmt.setObject(1, valueInet6Address);
    pstmt.addBatch();
    
    pstmt.executeBatch();
```
### DataTypeUUID
```java
    // pstmt is the object for PreparedStatement class
    
    String valueString = "cd175988-5fd2-11ec-bf63-0242ac130002";
    UUID valueUUID = UUID.fromString("cd175988-5fd2-11ec-bf63-0242ac130002");
    
    pstmt.setString(1, valueString);
    pstmt.addBatch();
    
    pstmt.setString(1, valueUUID.toString());
    pstmt.addBatch();
    
    pstmt.setObject(1, valueString);
    pstmt.addBatch();
    
    pstmt.setObject(1, valueUUID);
    pstmt.addBatch();
    
    pstmt.executeBatch();
```
### DataTypeDate
```java
    // pstmt is the object for PreparedStatement class
    
    Date valueDate = new Date(25369874L);
    LocalDate valueLocalDate = LocalDate.of(2012, 9, 9);
    
    pstmt.setDate(1, valueDate);
    pstmt.addBatch();
    
    pstmt.setDate(1, Date.valueOf(valueLocalDate));
    pstmt.addBatch();
    
    pstmt.setObject(1, valueDate);
    pstmt.addBatch();
    
    pstmt.setObject(1, valueLocalDate);
    pstmt.addBatch();
    
    pstmt.executeBatch();
```
### DataTypeDateTime
```java
    // pstmt is the object for PreparedStatement class
    
    ZonedDateTime valueZonedDateTime = ZonedDateTime.of(2012, 1, 1, 0, 0, 0, 0, ZoneId.of("Asia/Singapore"));
    Timestamp valueTimestamp = Timestamp.valueOf("2012-01-01 00:00:00");
    
    pstmt.setTimestamp(1, Timestamp.from(valueZonedDateTime.toInstant()));
    pstmt.addBatch();
    
    pstmt.setTimestamp(1, valueTimestamp);
    pstmt.addBatch();
    
    pstmt.setObject(1, valueZonedDateTime);
    pstmt.addBatch();
    
    pstmt.setObject(1, valueTimestamp);
    pstmt.addBatch();
    
    pstmt.executeBatch();
```
### DataTypeEnum8
```java
    // pstmt is the object for PreparedStatement class
    
    String valueString = "a";
    
    pstmt.setString(1, valueString);
    pstmt.addBatch();
    
    pstmt.executeBatch();
```
### DataTypeEnum16
```java
    // pstmt is the object for PreparedStatement class
    
    String valueString = "a";
    
    pstmt.setString(1, valueString);
    pstmt.addBatch();
    
    pstmt.executeBatch();
```
### DataTypeNullable
```java
    // pstmt is the object for PreparedStatement class
    
    String valueString = null;
    
    pstmt.setString(1, valueString);
    pstmt.addBatch();
    
    pstmt.executeBatch();
```
### DataTypeArray
#### ByteHouse Array
ByteHouseArray implements java.sql.array and is the object that you can use to ingest array types to ByteHouse. 
Construction of ByteHouseArray takes two parameters: DataType & Object[] (Object array containing the elements)
```java
    ByteHouseArray byteHouseArrayIntegers = new ByteHouseArray(new DataTypeInt32(), integersArray);
    ByteHouseArray byteHouseArrayInts = new ByteHouseArray(new DataTypeInt32(), intsArray);
    ByteHouseArray byteHouseArrayFloat32 = new ByteHouseArray(new DataTypeFloat32(), integersArray);
```
```java
    // pstmt is the object for PreparedStatement class
    
    Integer[] integers = new Integer[]{1, 2, 3};
    int[] ints =  new int[]{1, 2, 3};
    
    ByteHouseArray byteHouseArrayIntegers = new ByteHouseArray(new DataTypeInt32(), integers);
    ByteHouseArray byteHouseArrayInts = new ByteHouseArray(new DataTypeInt32(), ints);
    
    pstmt.setArray(1, byteHouseArrayIntegers);
    pstmt.addBatch();
    
    pstmt.setArray(1, byteHouseArrayInts);
    pstmt.addBatch();
    
    pstmt.setObject(1, byteHouseArrayIntegers);
    pstmt.addBatch();
    
    pstmt.setObject(1, byteHouseArrayInts);
    pstmt.addBatch();
    
    pstmt.executeBatch();
```
### DataTypeMap
```java
    // pstmt is the object for PreparedStatement class
    
    Map<Integer, Integer> values = new HashMap<>();
    values.put(1, 1);
    values.put(2, 2);
    values.put(3, 3);
    
    pstmt.setObject(1, values);
    pstmt.addBatch();
    
    pstmt.executeBatch();
```
### DataTypeTuple
#### ByteHouseStruct
ByteHouseArray implements java.sql.struct and is the object that you can use to ingest struct types to ByteHouse. 
Construction of ByteHouseStruct takes two parameters: DataType & Object[] (Object array containing the elements)
```java
// pstmt is the object for PreparedStatement class

ByteHouseStruct byteHouseStruct = new ByteHouseStruct("Tuple", new Object[]{"test_string", 1});

pstmt.setObject(1, byteHouseStruct);
pstmt.addBatch();

pstmt.executeBatch();
```
## Integration with BI Tools
### Tableau Integration
1. Download the Connector file (.taco)
2. Move the .taco file here:
   * Windows: C:\Users\[Windows User]\Documents\My Tableau Repository\Connectors
   * macOS: /Users/[user]/Documents/My Tableau Repository/Connectors
3. Start Tableau and under **Connect**, select the [ByteHouse JDBC Connector] connector. (**Note:** You’ll be prompted if the driver is not yet installed.
4. Driver Installation:
   * Go to the driver download page https://artifact.bytedance.com/repository/releases/com/bytedance/bytehouse/driver-java/
   * Click on the directory for the version that you need. The most recent version is not always at the end of the list.
   * Download the driver-java-#.#.#-all.jar file. Minimum required driver version: 1.1.0
   * Move jar file into the following directory:
     * Windows: C:\Program Files\Tableau\Drivers
     * macOS: /Users/[user]/Library/Tableau/Drivers
5. Relaunch Tableau and connect using the [ByteHouse JDBC Connector] connector.
### DataGrip Integration
### DBeaver Integration
## Parameters Reference
### Authentication Parameters
<table><tr><td class="selected" style="text-align: left; vertical-align: top;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>ACCOUNT</span></p></div></div></td><td class="selected" style="text-align: left; vertical-align: top;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>String type</span></p></div></div></td><td class="selected" style="text-align: left; vertical-align: top;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>denotes the bytehouse account you're connecting to</span></p></div></div></td></tr><tr><td class="selected" style="text-align: left; vertical-align: top;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>USER</span></p></div></div></td><td class="selected" style="text-align: left; vertical-align: top;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>String type</span></p></div></div></td><td class="selected" style="text-align: left; vertical-align: top;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>denotes the bytehouse user that you're connecting to</span></p></div></div></td></tr><tr><td class="selected" style="text-align: left; vertical-align: top;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>PASSWORD</span></p></div></div></td><td class="selected" style="text-align: left; vertical-align: top;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>String type</span></p></div></div></td><td class="selected" style="text-align: left; vertical-align: top;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>denotes the password for this account &amp; user</span></p></div></div></td></tr><tr><td class="selected" style="text-align: left; vertical-align: top;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>REGION</span></p></div></div></td><td class="selected" style="text-align: left; vertical-align: top;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>String type</span></p></div></div></td><td class="selected" style="text-align: left; vertical-align: top;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>denotes the region that you're connecting to</span></p></div></div></td></tr><tr><td class="selected" style="text-align: left; vertical-align: top;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>ACCESS_KEY</span></p></div></div></td><td class="selected" style="text-align: left; vertical-align: top;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>String type</span></p></div></div></td><td class="selected" style="text-align: left; vertical-align: top;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>denotes the access key for your volcano engine account</span></p></div></div></td></tr><tr><td class="selected" style="text-align: left; vertical-align: top;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>SECRET_KEY</span></p></div></div></td><td class="selected" style="text-align: left; vertical-align: top;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>String type</span></p></div></div></td><td class="selected" style="text-align: left; vertical-align: top;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>denotes the secret key for your volcano engine</span></p></div></div></td></tr><tr><td class="selected" style="text-align: left; vertical-align: top;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>IS_VOLCANO</span></p></div></div></td><td class="selected" style="text-align: left; vertical-align: top;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>Boolean type</span></p></div></div></td><td class="selected" style="text-align: left; vertical-align: top;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>set to true if you are connecting to volcano cloud using access_key &amp; secret_key</span></p></div></div></td></tr></table>

### Connection Parameters
<table><tr><td class="selected" style="text-align: start; vertical-align: top;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>Param name</span></p></div></div></td><td class="selected" style="text-align: start; vertical-align: top;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>Default value</span></p></div></div></td><td class="selected" style="text-align: start; vertical-align: top;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>Type</span></p></div></div></td><td class="selected" style="text-align: start; vertical-align: top;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>Description</span></p></div></div></td></tr><tr><td class="selected" style="text-align: start; vertical-align: top;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>secure</span></p></div></div></td><td class="selected" style="text-align: start; vertical-align: top;"><div class="wrap"><div style="margin: 10px 5px;"><p><span> </span></p></div></div></td><td class="selected" style="text-align: start; vertical-align: top;"><div class="wrap"><div style="margin: 10px 5px;"><p><span> </span></p></div></div></td><td class="selected" style="text-align: start; vertical-align: top;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>denotes whether the connection would use secure tcp/tls or not</span></p></div></div></td></tr><tr><td class="selected" style="text-align: start; vertical-align: top;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>queryTimeout</span></p></div></div></td><td class="selected" style="text-align: start; vertical-align: top;"><div class="wrap"><div style="margin: 10px 5px;"><p><span> </span></p></div></div></td><td class="selected" style="text-align: start; vertical-align: top;"><div class="wrap"><div style="margin: 10px 5px;"><p><span> </span></p></div></div></td><td class="selected" style="text-align: start; vertical-align: top;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>denotes query timeout value in seconds</span></p></div></div></td></tr><tr><td class="selected" style="text-align: start; vertical-align: top;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>connectTimeout</span></p></div></div></td><td class="selected" style="text-align: start; vertical-align: top;"><div class="wrap"><div style="margin: 10px 5px;"><p><span> </span></p></div></div></td><td class="selected" style="text-align: start; vertical-align: top;"><div class="wrap"><div style="margin: 10px 5px;"><p><span> </span></p></div></div></td><td class="selected" style="text-align: start; vertical-align: top;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>denotes connection timeout value in seconds</span></p></div></div></td></tr><tr><td class="selected" style="text-align: start; vertical-align: top;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>tcpKeepAlive</span></p></div></div></td><td class="selected" style="text-align: start; vertical-align: top;"><div class="wrap"><div style="margin: 10px 5px;"><p><span> </span></p></div></div></td><td class="selected" style="text-align: start; vertical-align: top;"><div class="wrap"><div style="margin: 10px 5px;"><p><span> </span></p></div></div></td><td class="selected" style="text-align: start; vertical-align: top;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>tcp connection properties</span></p></div></div></td></tr><tr><td class="selected" style="text-align: start; vertical-align: top;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>tcpNoDelay</span></p></div></div></td><td class="selected" style="text-align: start; vertical-align: top;"><div class="wrap"><div style="margin: 10px 5px;"><p><span> </span></p></div></div></td><td class="selected" style="text-align: start; vertical-align: top;"><div class="wrap"><div style="margin: 10px 5px;"><p><span> </span></p></div></div></td><td class="selected" style="text-align: start; vertical-align: top;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>tcp connection properties</span></p></div></div></td></tr><tr><td class="selected" style="text-align: start; vertical-align: top;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>enableCompression</span></p></div></div></td><td class="selected" style="text-align: start; vertical-align: top;"><div class="wrap"><div style="margin: 10px 5px;"><p><span> </span></p></div></div></td><td class="selected" style="text-align: start; vertical-align: top;"><div class="wrap"><div style="margin: 10px 5px;"><p><span> </span></p></div></div></td><td class="selected" style="text-align: start; vertical-align: top;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>denotes whether driver would use LZ4 compression or not</span></p></div></div></td></tr><tr><td class="selected" style="text-align: start; vertical-align: top;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>charset</span></p></div></div></td><td class="selected" style="text-align: start; vertical-align: top;"><div class="wrap"><div style="margin: 10px 5px;"><p><span> </span></p></div></div></td><td class="selected" style="text-align: start; vertical-align: top;"><div class="wrap"><div style="margin: 10px 5px;"><p><span> </span></p></div></div></td><td class="selected" style="text-align: start; vertical-align: top;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>denotes the character set used to encode or decode strings</span></p></div></div></td></tr><tr><td class="selected" style="text-align: start; vertical-align: top;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>max_block_size</span></p></div></div></td><td class="selected" style="text-align: start; vertical-align: top;"><div class="wrap"><div style="margin: 10px 5px;"><p><span> </span></p></div></div></td><td class="selected" style="text-align: start; vertical-align: top;"><div class="wrap"><div style="margin: 10px 5px;"><p><span> </span></p></div></div></td><td class="selected" style="text-align: start; vertical-align: top;"><div class="wrap"><div style="margin: 10px 5px;"><p><span>denotes the internal buffer size for the number of rows before sending it to the server </span></p></div></div></td></tr></table>

### Query / Server Side Parameters
Please refer to the ByteHouse documentation for available query / server side params.
## Troubleshooting
## Issue Reporting
If you have found a bug or if you have a feature request, please report them at this repository issues section. 
Alternatively, you can directly create an issue with our support platform here: https://bytehouse.cloud/support
## License
This project is distributed under the terms of the Apache License (Version 2.0). 
