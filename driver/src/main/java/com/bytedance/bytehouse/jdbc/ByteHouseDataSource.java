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

import com.bytedance.bytehouse.exception.InvalidValueException;
import com.bytedance.bytehouse.jdbc.wrapper.BHDataSource;
import com.bytedance.bytehouse.log.Logger;
import com.bytedance.bytehouse.log.LoggerFactoryUtils;
import com.bytedance.bytehouse.misc.StrUtil;
import com.bytedance.bytehouse.misc.ValidateUtils;
import com.bytedance.bytehouse.settings.ByteHouseConfig;
import com.bytedance.bytehouse.settings.SettingKey;
import java.io.PrintWriter;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

/**
 * <p> Database for bytehouse jdbc connections.
 * <p> It has list of database urls.
 * For every {@link #getConnection() getConnection} invocation, it returns connection to random host from the list.
 * Furthermore, this class has method { #scheduleActualization(int, TimeUnit) scheduleActualization}
 * which test hosts for availability. By default, this option is turned off.
 */
public final class ByteHouseDataSource implements BHDataSource {
    private static final Logger LOG = LoggerFactoryUtils.getLogger(ByteHouseDataSource.class);

    private final ByteHouseConfig cfg;

    private final ByteHouseDriver driver = new ByteHouseDriver();

    private final List<String> enabledUrls;

    /**
     * create Datasource for bytehouse JDBC connections
     *
     * @param url address for connection to the database, must have the next format
     *            {@code jdbc:bytehouse://<first-host>:<port>,<second-host>:<port>/<database>?param1=value1&param2=value2 }
     *            for example, {@code jdbc:bytehouse://localhost:9000,localhost:9000/database?compress=1&decompress=2 }
     * @throws IllegalArgumentException if param have not correct format,
     *                                  or error happens when checking host availability
     */
    public ByteHouseDataSource(String url) {
        this(url, new Properties());
    }

    /**
     * create Datasource for bytehouse JDBC connections
     *
     * @param url        address for connection to the database
     * @param properties database properties
     * @see #ByteHouseDataSource(String)
     */
    public ByteHouseDataSource(String url, Properties properties) {
        this(url, ByteHouseJdbcUrlParser.parseProperties(properties));
    }

    /**
     * create Datasource for bytehouse JDBC connections
     *
     * @param url      address for connection to the database
     * @param settings bytehouse settings
     * @see #ByteHouseDataSource(String)
     */
    private ByteHouseDataSource(
            final String url,
            final Map<SettingKey, Serializable> settings
    ) {
        final List<String> urls = splitUrl(url);

        this.cfg = ByteHouseConfig.Builder.builder()
                .withJdbcUrl(urls.get(0))
                .withSettings(settings)
                .host("toBeReplacedLater")
                .port(0)
                .build();

        final List<String> allUrls = new ArrayList<>(urls.size());
        for (final String u : urls) {
            try {
                if (driver.acceptsURL(u)) {
                    allUrls.add(u);
                } else {
                    LOG.warn("that url is has not correct format: {}", u);
                }
            } catch (Exception e) {
                throw new InvalidValueException("error while checking url: " + u, e);
            }
        }

        ValidateUtils.ensure(!allUrls.isEmpty(), "there are no correct urls");

        this.enabledUrls = Collections.unmodifiableList(allUrls);
    }

    /**
     * This method is kind of pointless. since there is always only 1 url.
     */
    static List<String> splitUrl(final String url) {
        ValidateUtils.ensure(
                url.startsWith(ByteHouseJdbcUrlParser.JDBC_PREFIX),
                "not JDBC url: " + url
        );
        final String bhUrl = url.substring(ByteHouseJdbcUrlParser.JDBC_PREFIX.length());
        ValidateUtils.ensure(
                bhUrl.startsWith(ByteHouseJdbcUrlParser.BYTEHOUSE_PREFIX),
                "not ByteHouse url: " + url);

        final URI uri;
        try {
            uri = new URI(bhUrl);
        } catch (URISyntaxException e) {
            throw new InvalidValueException("Invalid url: " + url);
        }
        final String database = StrUtil.getOrDefault(uri.getPath(), "");
        final String query = StrUtil.getOrDefault(uri.getQuery(), "");
        final String queryString = query.isEmpty() ? query : "?" + query;
        final String[] hosts = StrUtil.getOrDefault(uri.getAuthority(), "").split(",", -1);

        return Arrays.stream(hosts)
                .map(host -> "jdbc:" + uri.getScheme() + "://" + host + database + queryString)
                .collect(Collectors.toList());
    }

    @Override
    public boolean ping(int timeoutSecond) {
        try (ByteHouseConnection connection = getConnection()) {
            return connection.ping(Duration.ofSeconds(timeoutSecond));
        } catch (Exception e) {
            return false;
        }
    }

    private String getAnyUrl() throws SQLException {
        final List<String> localEnabledUrls = enabledUrls;
        if (localEnabledUrls.isEmpty()) {
            throw new SQLException("Unable to get connection: there are no enabled urls");
        }
        final int index = ThreadLocalRandom.current().nextInt(localEnabledUrls.size());
        return localEnabledUrls.get(index);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ByteHouseConnection getConnection() throws SQLException {
        return driver.connect(getAnyUrl(), cfg);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ByteHouseConnection getConnection(
            final String user,
            final String password
    ) throws SQLException {
        return driver.connect(getAnyUrl(), cfg.withCredentials(user, password));
    }

    /**
     * Logging for data source is disabled.
     */
    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return null;
    }

    /**
     * Not supported as logging cannot be enabled.
     */
    @Override
    public void setLogWriter(final PrintWriter printWriter) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    /**
     * Not supported. Consider using connectTimeout instead.
     */
    @Override
    public int getLoginTimeout() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    /**
     * Not supported. Consider using connectTimeout instead.
     */
    @Override
    public void setLoginTimeout(final int seconds) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }

    public ByteHouseConfig getCfg() {
        return cfg;
    }
}
