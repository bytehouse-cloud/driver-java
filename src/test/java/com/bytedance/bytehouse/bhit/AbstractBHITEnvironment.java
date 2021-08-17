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
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import javax.sql.DataSource;

/**
 * Abstract class to be inherited by all ByteHouse Integration tests.
 * Contains methods for obtaining parameters to connect to ByteHouse.
 */
public abstract class AbstractBHITEnvironment {

    private static final String ACCOUNT_ID = "ACCOUNT_ID";

    private static final String USER = "USER";

    private static final String PASSWORD = "PASSWORD";

    private static final String SECURE = "SECURE";

    private static final String HOST = "HOST";

    private static final String PORT = "PORT";

    private static final String WAREHOUSE = "WAREHOUSE";

    private static final String DATABASE = "DATABASE";

    private Properties ByteHouseTestConfigs;

    protected String getEnvUsername() {
        return ByteHouseTestConfigs.getProperty(USER);
    }

    protected String getEnvPassword() {
        return ByteHouseTestConfigs.getProperty(PASSWORD);
    }

    protected Boolean isEnvSecureConnection() {
        return Boolean.parseBoolean(ByteHouseTestConfigs.getProperty(SECURE));
    }

    protected String getEnvAccountId() {
        return ByteHouseTestConfigs.getProperty(ACCOUNT_ID);
    }

    protected String getEnvUrl() {
        return String.format("jdbc:bytehouse://%s:%s",
                ByteHouseTestConfigs.getProperty(HOST),
                ByteHouseTestConfigs.getProperty(PORT)
        );
    }

    protected String getEnvVirtualWarehouse() {
        return ByteHouseTestConfigs.getProperty(WAREHOUSE);
    }

    protected String getEnvDatabase() {
        return ByteHouseTestConfigs.getProperty(DATABASE);
    }

    protected Connection getEnvConnection() throws SQLException {
        loadByteHouseTestConfigs();

        final Properties properties = new Properties();
        properties.setProperty("account_id", getEnvAccountId());
        properties.setProperty("user", getEnvUsername());
        properties.setProperty("password", getEnvPassword());
        properties.setProperty("secure", String.valueOf(isEnvSecureConnection()));
        final DataSource dataSource = new BalancedByteHouseDataSource(getEnvUrl(), properties);
        return dataSource.getConnection();
    }

    private void loadByteHouseTestConfigs() {
        try (InputStream input = Files.newInputStream(Paths
                .get("src/test/resources/config.properties"))) {
            ByteHouseTestConfigs = new Properties();
            ByteHouseTestConfigs.load(input);
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    protected String loadSqlStatement(final String filepath) throws IOException {
        final String fullPath = "src/test/resources/sql/" + filepath + ".sql";
        final byte[] bytes = Files.readAllBytes(Paths.get(fullPath));
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
