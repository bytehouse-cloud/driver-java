/*
 * This file may have been modified by ByteDance Ltd. and/or its affiliates.
 *
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

import com.bytedance.bytehouse.exception.ByteHouseClientException;
import com.bytedance.bytehouse.settings.BHConstants;
import com.bytedance.bytehouse.settings.ByteHouseConfig;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;

/**
 * The driver of ByteHouse, entry point of the application.
 */
public class ByteHouseDriver implements Driver {

    static {
        try {
            DriverManager.registerDriver(new ByteHouseDriver());
        } catch (SQLException e) {
            throw new ByteHouseClientException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean acceptsURL(final String url) throws SQLException {
        if (url == null) {
            // FIXME: 10/8/21 throw internal sql exception
            throw new SQLException("url is null");
        }
        return url.startsWith(ByteHouseJdbcUrlParser.JDBC_BYTEHOUSE_PREFIX);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ByteHouseConnection connect(
            final String url,
            final Properties properties
    ) throws SQLException {
        if (!this.acceptsURL(url)) {
            return null;
        }

        final ByteHouseConfig cfg = ByteHouseConfig.Builder.builder()
                .withJdbcUrl(url)
                .withProperties(properties)
                .build();
        return ByteHouseConnection.createByteHouseConnection(cfg);
    }

    ByteHouseConnection connect(final String url, final ByteHouseConfig cfg) throws SQLException {
        if (!this.acceptsURL(url)) {
            // FIXME: 10/8/21 throw exception
            return null;
        }
        final ByteHouseConfig newConfig = cfg.withJdbcUrl(url);
        return ByteHouseConnection.createByteHouseConnection(newConfig);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DriverPropertyInfo[] getPropertyInfo(final String url, final Properties properties) {
        final ByteHouseConfig cfg = ByteHouseConfig.Builder.builder()
                .withJdbcUrl(url)
                .withProperties(properties)
                .build();
        return cfg.settings().entrySet().stream().map(entry -> {
            final DriverPropertyInfo property = new DriverPropertyInfo(
                    entry.getKey().name(),
                    String.valueOf(entry.getValue())
            );
            property.description = entry.getKey().description();
            return property;
        }).toArray(DriverPropertyInfo[]::new);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getMajorVersion() {
        return BHConstants.MAJOR_VERSION;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getMinorVersion() {
        return BHConstants.MINOR_VERSION;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }
}
