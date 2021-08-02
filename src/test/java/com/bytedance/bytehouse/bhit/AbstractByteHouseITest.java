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
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Abstract class to be inherited by all ByteHouse Integration tests.
 * Contains methods for obtaining parameters to connect to ByteHouse.
 *
 *
 */
@ByteHouseIntegrationTest
public abstract class AbstractByteHouseITest {

    /**
     * The following environment variables should be defined for the BH integration tests.
     */
    private static final String USER = "USER";
    private static final String PASSWORD = "PASSWORD";
    private static final String SECURE = "SECURE";
    private static final String HOST = "HOST";
    private static final String PORT = "PORT";
    private static final String WAREHOUSE = "WAREHOUSE";

    protected String getUsername() {
        return System.getenv(USER);
    }

    protected String getPassword() {
        return System.getenv(PASSWORD);
    }

    protected Boolean isSecureConnection() {
        return Boolean.parseBoolean(System.getenv(SECURE));
    }

    protected String getUrl() {
        return String.format("jdbc:bytehouse://%s:%s", System.getenv(HOST), System.getenv(PORT));
    }

    protected String getVirtualWarehouse() throws SQLException {
        return System.getenv(WAREHOUSE);
    }

    protected Connection getConnection() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty("user", getUsername());
        properties.setProperty("password", getPassword());
        properties.setProperty("secure", String.valueOf(isSecureConnection()));
        DataSource dataSource = new BalancedByteHouseDataSource(getUrl(), properties);
        return dataSource.getConnection();
    }
}
