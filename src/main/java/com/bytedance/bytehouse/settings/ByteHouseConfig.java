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
package com.bytedance.bytehouse.settings;

import com.bytedance.bytehouse.jdbc.ByteHouseJdbcUrlParser;
import com.bytedance.bytehouse.misc.CollectionUtil;
import com.bytedance.bytehouse.misc.StrUtil;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import javax.annotation.concurrent.Immutable;

@Immutable
public class ByteHouseConfig implements Serializable {

    private final String host;

    private final int port;

    private final String database;

    private final String accountId;

    private final String user;

    private final String password;

    private final Duration queryTimeout;

    private final Duration connectTimeout;

    private final boolean tcpKeepAlive;

    private final boolean tcpNoDelay;

    private final boolean secure;

    private final boolean skipVerification;

    private final boolean enableCompression;

    private final String charset; // use String because Charset is not serializable

    private final Map<SettingKey, Serializable> settings;

    private ByteHouseConfig(String host, int port, String database, String accountId, String user, String password,
                            Duration queryTimeout, Duration connectTimeout, boolean tcpKeepAlive, boolean tcpNoDelay,
                            boolean secure, boolean skipVerification, boolean enableCompression, String charset,
                            Map<SettingKey, Serializable> settings) {
        this.host = host;
        this.port = port;
        this.database = database;
        this.accountId = accountId;
        this.user = user;
        this.password = password;
        this.queryTimeout = queryTimeout;
        this.connectTimeout = connectTimeout;
        this.tcpKeepAlive = tcpKeepAlive;
        this.tcpNoDelay = tcpNoDelay;
        this.secure = secure;
        this.skipVerification = skipVerification;
        this.enableCompression = enableCompression;
        this.charset = charset;
        this.settings = settings;
    }

    public String host() {
        return this.host;
    }

    public int port() {
        return this.port;
    }

    public String database() {
        return this.database;
    }

    public String accountId() {
        return this.accountId;
    }

    public String user() {
        return this.user;
    }

    public String password() {
        return this.password;
    }

    public Duration queryTimeout() {
        return this.queryTimeout;
    }

    public Duration connectTimeout() {
        return this.connectTimeout;
    }

    public boolean tcpKeepAlive() {
        return tcpKeepAlive;
    }

    public boolean tcpNoDelay() {
        return tcpNoDelay;
    }

    public boolean secure() {
        return secure;
    }

    public boolean skipVerification() {
        return skipVerification;
    }

    public boolean enableCompression() {
        return enableCompression;
    }

    public Charset charset() {
        return Charset.forName(charset);
    }

    public Map<SettingKey, Serializable> settings() {
        return settings;
    }

    public String fullUsername() {
        if (StrUtil.isBlank(this.accountId)) {
            return this.user;
        }
        return String.format("%s::%s", this.accountId, this.user);
    }

    public String jdbcUrl() {
        StringBuilder builder = new StringBuilder(ByteHouseJdbcUrlParser.JDBC_BYTEHOUSE_PREFIX)
                .append("//").append(host).append(":").append(port).append("/").append(database)
                .append("?").append(SettingKey.query_timeout.name()).append("=").append(queryTimeout.getSeconds())
                .append("&").append(SettingKey.connect_timeout.name()).append("=").append(connectTimeout.getSeconds())
                .append("&").append(SettingKey.charset.name()).append("=").append(charset)
                .append("&").append(SettingKey.tcp_keep_alive.name()).append("=").append(tcpKeepAlive)
                .append("&").append(SettingKey.tcp_no_delay.name()).append("=").append(tcpNoDelay)
                .append("&").append(SettingKey.secure.name()).append("=").append(secure)
                .append("&").append(SettingKey.skip_verification.name()).append("=").append(skipVerification)
                .append("&").append(SettingKey.enableCompression.name()).append("=").append(enableCompression);

        for (Map.Entry<SettingKey, Serializable> entry : settings.entrySet()) {
            builder.append("&").append(entry.getKey().name()).append("=").append(entry.getValue());
        }
        return builder.toString();
    }

    public ByteHouseConfig withHostPort(String host, int port) {
        return Builder.builder(this)
                .host(host)
                .port(port)
                .build();
    }

    public ByteHouseConfig withDatabase(String database) {
        return Builder.builder(this)
                .database(database)
                .build();
    }

    public ByteHouseConfig withAccountId(String accountId) {
        return Builder.builder(this)
                .accountId(accountId)
                .build();
    }

    public ByteHouseConfig withCredentials(String user, String password) {
        return Builder.builder(this)
                .user(user)
                .password(password)
                .build();
    }

    public ByteHouseConfig withQueryTimeout(Duration timeout) {
        return Builder.builder(this)
                .queryTimeout(timeout)
                .build();
    }

    public ByteHouseConfig withConnectTimeout(Duration timeout) {
        return Builder.builder(this)
                .connectTimeout(timeout)
                .build();
    }

    public ByteHouseConfig withTcpKeepAlive(boolean enable) {
        return Builder.builder(this)
                .tcpKeepAlive(enable)
                .build();
    }

    public ByteHouseConfig withTcpNoDelay(boolean tcpNoDelay) {
        return Builder.builder(this)
                .tcpNoDelay(tcpNoDelay)
                .build();
    }

    public ByteHouseConfig withSecure(boolean secure) {
        return Builder.builder(this)
                .secure(secure)
                .build();
    }

    public ByteHouseConfig withSkipVerification(boolean skipVerification) {
        return Builder.builder(this)
                .skipVerification(skipVerification)
                .build();
    }

    public ByteHouseConfig withEnableCompression(boolean enableCompression) {
        return Builder.builder(this)
                .enableCompression(enableCompression)
                .build();
    }

    public ByteHouseConfig withCharset(Charset charset) {
        return Builder.builder(this)
                .charset(charset)
                .build();
    }

    public ByteHouseConfig withSettings(Map<SettingKey, Serializable> settings) {
        return Builder.builder(this)
                .withSettings(settings)
                .build();
    }

    public ByteHouseConfig withJdbcUrl(String url) {
        return Builder.builder(this)
                .withJdbcUrl(url)
                .build();
    }

    public ByteHouseConfig withProperties(Properties properties) {
        return Builder.builder(this)
                .withProperties(properties)
                .build();
    }

    public ByteHouseConfig with(String url, Properties properties) {
        return Builder.builder(this)
                .withJdbcUrl(url)
                .withProperties(properties)
                .build();
    }

    public static final class Builder {

        private String host;

        private int port;

        private String database;

        private String accountId;

        private String user;

        private String password;

        private Duration queryTimeout;

        private Duration connectTimeout;

        private boolean tcpKeepAlive;

        private boolean tcpNoDelay;

        private boolean secure;

        private boolean skipVerification;

        private boolean enableCompression;

        private Charset charset;

        private Map<SettingKey, Serializable> settings = new HashMap<>();

        private Builder() {
        }

        public static Builder builder() {
            return new Builder();
        }

        public static Builder builder(ByteHouseConfig cfg) {
            return new Builder()
                    .host(cfg.host())
                    .port(cfg.port())
                    .database(cfg.database())
                    .accountId(cfg.accountId())
                    .user(cfg.user())
                    .password(cfg.password())
                    .queryTimeout(cfg.queryTimeout())
                    .connectTimeout(cfg.connectTimeout())
                    .tcpKeepAlive(cfg.tcpKeepAlive())
                    .tcpNoDelay(cfg.tcpNoDelay())
                    .secure(cfg.secure())
                    .skipVerification(cfg.skipVerification())
                    .enableCompression(cfg.enableCompression())
                    .charset(cfg.charset())
                    .withSettings(cfg.settings());
        }

        public Builder withSetting(SettingKey key, Serializable value) {
            this.settings.put(key, value);
            return this;
        }

        public Builder withSettings(Map<SettingKey, Serializable> settings) {
            CollectionUtil.mergeMapInPlaceKeepLast(this.settings, settings);
            return this;
        }

        public Builder host(String host) {
            this.withSetting(SettingKey.host, host);
            return this;
        }

        public Builder port(int port) {
            this.withSetting(SettingKey.port, port);
            return this;
        }

        public Builder database(String database) {
            this.withSetting(SettingKey.database, database);
            return this;
        }

        public Builder accountId(String accountId) {
            this.withSetting(SettingKey.account_id, accountId);
            return this;
        }

        public Builder user(String user) {
            this.withSetting(SettingKey.user, user);
            return this;
        }

        public Builder password(String password) {
            this.withSetting(SettingKey.password, password);
            return this;
        }

        public Builder queryTimeout(Duration queryTimeout) {
            this.withSetting(SettingKey.query_timeout, queryTimeout);
            return this;
        }

        public Builder connectTimeout(Duration connectTimeout) {
            this.withSetting(SettingKey.connect_timeout, connectTimeout);
            return this;
        }

        public Builder tcpKeepAlive(boolean tcpKeepAlive) {
            this.withSetting(SettingKey.tcp_keep_alive, tcpKeepAlive);
            return this;
        }

        public Builder tcpNoDelay(boolean tcpNoDelay) {
            this.withSetting(SettingKey.tcp_no_delay, tcpNoDelay);
            return this;
        }

        public Builder secure(boolean secure) {
            this.withSetting(SettingKey.secure, secure);
            return this;
        }

        public Builder skipVerification(boolean skipVerification) {
            this.withSetting(SettingKey.skip_verification, skipVerification);
            return this;
        }

        public Builder enableCompression(boolean enableCompression) {
            this.withSetting(SettingKey.enableCompression, enableCompression);
            return this;
        }

        public Builder charset(String charset) {
            this.withSetting(SettingKey.charset, charset);
            return this;
        }

        public Builder charset(Charset charset) {
            this.withSetting(SettingKey.charset, charset.name());
            return this;
        }

        public Builder settings(Map<SettingKey, Serializable> settings) {
            this.settings = settings;
            return this;
        }

        public Builder clearSettings() {
            this.settings = new HashMap<>();
            return this;
        }

        public Builder withJdbcUrl(String jdbcUrl) {
            return this.withSettings(ByteHouseJdbcUrlParser.parseJdbcUrl(jdbcUrl));
        }

        public Builder withProperties(Properties properties) {
            return this.withSettings(ByteHouseJdbcUrlParser.parseProperties(properties));
        }

        public ByteHouseConfig build() {
            this.host = (String) this.settings.getOrDefault(SettingKey.host, "127.0.0.1");
            this.port = ((Number) this.settings.getOrDefault(SettingKey.port, 9000)).intValue();
            this.database = (String) this.settings.getOrDefault(SettingKey.database, "default");
            this.accountId = (String) this.settings.getOrDefault(SettingKey.account_id, "");
            this.user = (String) this.settings.getOrDefault(SettingKey.user, "default");
            this.password = (String) this.settings.getOrDefault(SettingKey.password, "");
            this.queryTimeout = (Duration) this.settings.getOrDefault(SettingKey.query_timeout, Duration.ZERO);
            this.connectTimeout = (Duration) this.settings.getOrDefault(SettingKey.connect_timeout, Duration.ZERO);
            this.tcpKeepAlive = (boolean) this.settings.getOrDefault(SettingKey.tcp_keep_alive, false);
            this.tcpNoDelay = (boolean) this.settings.getOrDefault(SettingKey.tcp_no_delay, true);
            this.secure = (boolean) this.settings.getOrDefault(SettingKey.secure, false);
            this.skipVerification = (boolean) this.settings.getOrDefault(SettingKey.skip_verification, false);
            this.enableCompression = (boolean) this.settings.getOrDefault(SettingKey.enableCompression, false);
            this.charset = Charset.forName((String) this.settings.getOrDefault(SettingKey.charset, "UTF-8"));

            revisit();
            purgeSettings();

            return new ByteHouseConfig(
                    host, port, database, accountId, user, password, queryTimeout,
                    connectTimeout, tcpKeepAlive, tcpNoDelay, secure, skipVerification, enableCompression,
                    charset.name(), settings
            );
        }

        private void revisit() {
            if (StrUtil.isBlank(this.host)) this.host = "127.0.0.1";
            if (this.port == -1) this.port = 9000;
            if (StrUtil.isBlank(this.database)) this.database = "default";
            if (StrUtil.isBlank(this.accountId)) this.accountId = "";
            if (StrUtil.isBlank(this.user)) this.user = "default";
            if (StrUtil.isBlank(this.password)) this.password = "";
            if (this.queryTimeout.isNegative()) this.queryTimeout = Duration.ZERO;
            if (this.connectTimeout.isNegative()) this.connectTimeout = Duration.ZERO;
        }

        private void purgeSettings() {
            this.settings.remove(SettingKey.host);
            this.settings.remove(SettingKey.port);
            this.settings.remove(SettingKey.database);
            this.settings.remove(SettingKey.account_id);
            this.settings.remove(SettingKey.user);
            this.settings.remove(SettingKey.password);
            this.settings.remove(SettingKey.query_timeout);
            this.settings.remove(SettingKey.connect_timeout);
            this.settings.remove(SettingKey.tcp_keep_alive);
            this.settings.remove(SettingKey.tcp_no_delay);
            this.settings.remove(SettingKey.secure);
            this.settings.remove(SettingKey.skip_verification);
            this.settings.remove(SettingKey.enableCompression);
            this.settings.remove(SettingKey.charset);
        }
    }
}
