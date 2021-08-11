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
package com.bytedance.bytehouse.bhit;

import com.bytedance.bytehouse.jdbc.BalancedByteHouseDataSource;
import javax.sql.DataSource;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Abstract class to be inherited by all ByteHouse Integration tests.
 * Contains methods for obtaining parameters to connect to ByteHouse.
 */
public abstract class AbstractByteHouseITest {

    private static final String ACCOUNT_ID = "ACCOUNT_ID";

    private static final String USER = "USER";

    private static final String PASSWORD = "PASSWORD";

    private static final String SECURE = "SECURE";

    private static final String HOST = "HOST";

    private static final String PORT = "PORT";

    private static final String WAREHOUSE = "WAREHOUSE";

    private static final String DATABASE = "DATABASE";

    private static Properties ByteHouseTestConfigs;

    protected String getUsername() {
        return ByteHouseTestConfigs.getProperty(USER);
    }

    protected String getPassword() {
        return ByteHouseTestConfigs.getProperty(PASSWORD);
    }

    protected Boolean isSecureConnection() {
        return Boolean.parseBoolean(ByteHouseTestConfigs.getProperty(SECURE));
    }

    protected String getAccountId() {
        return ByteHouseTestConfigs.getProperty(ACCOUNT_ID);
    }

    protected String getUrl() {
        return String.format("jdbc:bytehouse://%s:%s",
                ByteHouseTestConfigs.getProperty(HOST),
                ByteHouseTestConfigs.getProperty(PORT)
        );
    }

    protected String getVirtualWarehouse() {
        return ByteHouseTestConfigs.getProperty(WAREHOUSE);
    }

    protected String getDatabase() {
        return ByteHouseTestConfigs.getProperty(DATABASE);
    }

    protected Connection getConnection() throws SQLException {
        loadByteHouseTestConfigs();

        Properties properties = new Properties();
        properties.setProperty("account_id", getAccountId());
        properties.setProperty("user", getUsername());
        properties.setProperty("password", getPassword());
        properties.setProperty("secure", String.valueOf(isSecureConnection()));
        properties.setProperty("database", getDatabase());
        properties.setProperty("warehouse", getVirtualWarehouse());
        final DataSource dataSource = new BalancedByteHouseDataSource(getUrl(), properties);
        return dataSource.getConnection();
    }

    private void loadByteHouseTestConfigs() {
        try (InputStream input = new FileInputStream("src/test/resources/config.properties")) {
            ByteHouseTestConfigs = new Properties();
            ByteHouseTestConfigs.load(input);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    protected String loadSqlStatement(String filepath) throws IOException {
        String fullPath = "src/test/resources/sql/" + filepath + ".sql";
        File file = new File(fullPath);
        byte[] data;
        try (FileInputStream fis = new FileInputStream(file)) {
            data = new byte[(int) file.length()];
            fis.read(data);
        }
        String query = new String(data, StandardCharsets.UTF_8);
        return query;
    }
}
