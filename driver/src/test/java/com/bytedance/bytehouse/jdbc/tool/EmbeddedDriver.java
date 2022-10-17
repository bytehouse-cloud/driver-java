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

package com.bytedance.bytehouse.jdbc.tool;

import org.mockito.Mockito;

import java.sql.*;
import java.util.Properties;

public class EmbeddedDriver implements Driver {

    public static final Connection MOCKED_CONNECTION = Mockito.mock(Connection.class);
    public static final String EMBEDDED_DRIVER_PREFIX = "jdbc:embedded:";

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        return MOCKED_CONNECTION;
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return url.startsWith(EMBEDDED_DRIVER_PREFIX);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
        return 0;
    }

    @Override
    public int getMinorVersion() {
        return 0;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return null;
    }
}
