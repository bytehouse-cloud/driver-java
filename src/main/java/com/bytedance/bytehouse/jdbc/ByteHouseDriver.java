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

package com.bytedance.bytehouse.jdbc;

import com.bytedance.bytehouse.settings.ByteHouseConfig;
import com.bytedance.bytehouse.settings.ByteHouseDefines;
import com.bytedance.bytehouse.settings.SettingKey;

import java.io.Serializable;
import java.sql.*;
import java.util.Map;
import java.util.Properties;

public class ByteHouseDriver implements Driver {

    static {
        try {
            DriverManager.registerDriver(new ByteHouseDriver());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        if (url == null) {
            throw new SQLException("url is null");
        }
        return url.startsWith(ByteHouseJdbcUrlParser.JDBC_BYTEHOUSE_PREFIX);
    }

    @Override
    public ByteHouseConnection connect(String url, Properties properties) throws SQLException {
        if (!this.acceptsURL(url)) {
            return null;
        }

        ByteHouseConfig cfg = ByteHouseConfig.Builder.builder()
                .withJdbcUrl(url)
                .withProperties(properties)
                .build();
        return connect(url, cfg);
    }

    ByteHouseConnection connect(String url, ByteHouseConfig cfg) throws SQLException {
        if (!this.acceptsURL(url)) {
            return null;
        }
        return ByteHouseConnection.createByteHouseConnection(cfg.withJdbcUrl(url));
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties properties) throws SQLException {
        ByteHouseConfig cfg = ByteHouseConfig.Builder.builder()
                .withJdbcUrl(url)
                .withProperties(properties)
                .build();
        int index = 0;
        DriverPropertyInfo[] driverPropertiesInfo = new DriverPropertyInfo[cfg.settings().size()];

        for (Map.Entry<SettingKey, Serializable> entry : cfg.settings().entrySet()) {
            String value = String.valueOf(entry.getValue());

            DriverPropertyInfo property = new DriverPropertyInfo(entry.getKey().name(), value);
            property.description = entry.getKey().description();

            driverPropertiesInfo[index++] = property;
        }

        return driverPropertiesInfo;
    }

    @Override
    public int getMajorVersion() {
        return ByteHouseDefines.MAJOR_VERSION;
    }

    @Override
    public int getMinorVersion() {
        return ByteHouseDefines.MINOR_VERSION;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }
}
