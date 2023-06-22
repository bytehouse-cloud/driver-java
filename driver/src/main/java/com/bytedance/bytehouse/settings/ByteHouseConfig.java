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
package com.bytedance.bytehouse.settings;

import com.bytedance.bytehouse.exception.InvalidValueException;
import com.bytedance.bytehouse.jdbc.ByteHouseJdbcUrlParser;
import com.bytedance.bytehouse.misc.CollectionUtil;
import com.bytedance.bytehouse.misc.StrUtil;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.concurrent.Immutable;

/**
 * Immutable Configuration.
 */
@Immutable
public class ByteHouseConfig implements Serializable {

    private static final String apiKeyUser = "bytehouse";

    private final String region;

    private final String host;

    private final int port;

    private final String database;

    private final String account;

    private final String user;

    private final String password;

    private final String accessKey;

    private final String secretKey;

    private final String apiKey;

    private final boolean isTableau;

    private final boolean isVolcano;

    private final Duration queryTimeout;

    private final Duration connectTimeout;

    private final boolean tcpKeepAlive;

    private final boolean tcpNoDelay;

    private final boolean secure;

    private final String formatCSVDelimiter;

    private final boolean skipVerification;

    private final boolean enableCompression;

    private final String charset; // use String because Charset is not serializable

    private final long maxBlockSize;

    private final String booleanColumnPrefix;

    private final boolean insertInfileLocal;

    private final Map<SettingKey, Serializable> settings;

    private ByteHouseConfig(
            final String region,
            final String host,
            final int port,
            final String database,
            final String account,
            final String user,
            final String password,
            final String accessKey,
            final String secretKey,
            final String apiKey,
            final boolean isTableau,
            final boolean isVolcano,
            final Duration queryTimeout,
            final Duration connectTimeout,
            final boolean tcpKeepAlive,
            final boolean tcpNoDelay,
            final boolean secure,
            final String formatCSVDelimiter,
            final boolean skipVerification,
            final boolean enableCompression,
            final String charset,
            final long maxBlockSize,
            final String booleanColumnPrefix,
            final boolean insertInfileLocal,
            final Map<SettingKey, Serializable> settings
    ) {
        this.region = region;
        this.host = host;
        this.port = port;
        this.database = database;
        this.account = account;
        this.user = user;
        this.password = password;
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.apiKey = apiKey;
        this.isTableau = isTableau;
        this.isVolcano = isVolcano;
        this.queryTimeout = queryTimeout;
        this.connectTimeout = connectTimeout;
        this.tcpKeepAlive = tcpKeepAlive;
        this.tcpNoDelay = tcpNoDelay;
        this.secure = secure;
        this.formatCSVDelimiter = formatCSVDelimiter;
        this.skipVerification = skipVerification;
        this.enableCompression = enableCompression;
        this.charset = charset;
        this.maxBlockSize = maxBlockSize;
        this.booleanColumnPrefix = booleanColumnPrefix;
        this.insertInfileLocal = insertInfileLocal;
        this.settings = settings;
    }

    public String region() {
        return this.region;
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

    public String account() {
        return this.account;
    }

    public String user() {
        if (!StrUtil.isBlank(this.apiKey)) {
            return apiKeyUser;
        }
        return this.user;
    }

    public String password() {
        if (!StrUtil.isBlank(this.apiKey)) {
            return this.apiKey;
        }
        return this.password;
    }

    public String accessKey() {
        return this.accessKey;
    }

    public String secretKey() {
        return this.secretKey;
    }

    public String apiKey() {
        return this.apiKey;
    }

    public boolean isTableau() {
        return this.isTableau;
    }

    public boolean isVolcano() {
        return this.isVolcano;
    }

    public boolean satisfyVolcanoAttributes() {
        return isVolcano()  && !Objects.equals(accessKey(), "") && !Objects.equals(secretKey, "");
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

    public String formatCSVDelimiter() {
        return formatCSVDelimiter;
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

    public long maxBlockSize() {
        return maxBlockSize;
    }

    public String booleanColumnPrefix() {
        return booleanColumnPrefix;
    }

    public boolean insertInfileLocal() {
        return insertInfileLocal;
    }

    public Map<SettingKey, Serializable> settings() {
        return settings;
    }

    public String fullUsername() {
        if (!StrUtil.isBlank(this.apiKey)) {
            return apiKeyUser;
        }
        if (StrUtil.isBlank(this.account)) {
            return this.user;
        }
        return String.format("%s::%s", this.account, this.user);
    }

    public String jdbcUrl() {
        final StringBuilder builder = new StringBuilder(
                ByteHouseJdbcUrlParser.JDBC_BYTEHOUSE_PREFIX
        )
                .append("//").append(host).append(':').append(port).append('/').append(database)
                .append('?').append(SettingKey.queryTimeout.name()).append('=').append(queryTimeout.getSeconds())
                .append('&').append(SettingKey.connectTimeout.name()).append('=').append(connectTimeout.getSeconds())
                .append('&').append(SettingKey.charset.name()).append('=').append(charset)
                .append('&').append(SettingKey.tcpKeepAlive.name()).append('=').append(tcpKeepAlive)
                .append('&').append(SettingKey.tcpNoDelay.name()).append('=').append(tcpNoDelay)
                .append('&').append(SettingKey.secure.name()).append('=').append(secure)
                .append('&').append(SettingKey.skipVerification.name()).append('=').append(skipVerification)
                .append('&').append(SettingKey.enableCompression.name()).append('=').append(enableCompression);

        for (final Map.Entry<SettingKey, Serializable> entry : settings.entrySet()) {
            builder.append('&').append(entry.getKey().name()).append('=').append(entry.getValue());
        }
        return builder.toString();
    }

    /**
     * cloning method.
     */
    public ByteHouseConfig withHostPort(final String host, final int port) {
        return Builder.builder(this)
                .host(host)
                .port(port)
                .build();
    }

    /**
     * cloning method.
     */
    public ByteHouseConfig withDatabase(final String database) {
        return Builder.builder(this)
                .database(database)
                .build();
    }

    /**
     * cloning method.
     */
    public ByteHouseConfig withAccount(final String account) {
        return Builder.builder(this)
                .account(account)
                .build();
    }

    /**
     * cloning method.
     */
    public ByteHouseConfig withCredentials(final String user, final String password) {
        return Builder.builder(this)
                .user(user)
                .password(password)
                .build();
    }

    /**
     * cloning method.
     */
    public ByteHouseConfig withAKSKCredentials(final String accessKey, final String secretKey) {
        return Builder.builder(this)
                .accessKey(accessKey)
                .secretKey(secretKey)
                .build();
    }

    public ByteHouseConfig withAPIKeyCredentials(final String apiKey) {
        return Builder.builder(this)
                .apiKey(apiKey)
                .build();
    }

    /**
     * cloning method.
     */
    public ByteHouseConfig withIsTableau(final boolean isTableau) {
        return Builder.builder(this)
                .isTableau(isTableau)
                .build();
    }

    /**
     * cloning method.
     */
    public ByteHouseConfig withIsVolcano(final boolean isVolcano) {
        return Builder.builder(this)
                .isVolcano(isVolcano)
                .build();
    }

    /**
     * cloning method.
     */
    public ByteHouseConfig withQueryTimeout(final Duration timeout) {
        return Builder.builder(this)
                .queryTimeout(timeout)
                .build();
    }

    /**
     * cloning method.
     */
    public ByteHouseConfig withConnectTimeout(final Duration timeout) {
        return Builder.builder(this)
                .connectTimeout(timeout)
                .build();
    }

    /**
     * cloning method.
     */
    public ByteHouseConfig withTcpKeepAlive(final boolean enable) {
        return Builder.builder(this)
                .tcpKeepAlive(enable)
                .build();
    }

    /**
     * cloning method.
     */
    public ByteHouseConfig withTcpNoDelay(final boolean tcpNoDelay) {
        return Builder.builder(this)
                .tcpNoDelay(tcpNoDelay)
                .build();
    }

    /**
     * cloning method.
     */
    public ByteHouseConfig withSecure(final boolean secure) {
        return Builder.builder(this)
                .secure(secure)
                .build();
    }

    public ByteHouseConfig withFormatCSVDelimiter(final String formatCSVDelimiter) {
        return Builder.builder(this)
                .formatCSVDelimiter(formatCSVDelimiter)
                .build();
    }

    /**
     * cloning method.
     */
    public ByteHouseConfig withSkipVerification(final boolean skipVerification) {
        return Builder.builder(this)
                .skipVerification(skipVerification)
                .build();
    }

    /**
     * cloning method.
     */
    public ByteHouseConfig withEnableCompression(final boolean enableCompression) {
        return Builder.builder(this)
                .enableCompression(enableCompression)
                .build();
    }

    /**
     * cloning method.
     */
    public ByteHouseConfig withInsertInfileLocal(final boolean insertInfileLocal) {
        return Builder.builder(this)
                .insertInfileLocal(insertInfileLocal)
                .build();
    }

    /**
     * cloning method.
     */
    public ByteHouseConfig withCharset(final Charset charset) {
        return Builder.builder(this)
                .charset(charset)
                .build();
    }

    /**
     * cloning method.
     */
    public ByteHouseConfig withSettings(final Map<SettingKey, Serializable> settings) {
        return Builder.builder(this)
                .withSettings(settings)
                .build();
    }

    /**
     * cloning method.
     */
    public ByteHouseConfig withJdbcUrl(final String url) {
        return Builder.builder(this)
                .withJdbcUrl(url)
                .build();
    }

    /**
     * cloning method.
     */
    public ByteHouseConfig withProperties(final Properties properties) {
        return Builder.builder(this)
                .withProperties(properties)
                .build();
    }

    /**
     * cloning method.
     */
    public ByteHouseConfig with(final String url, final Properties properties) {
        return Builder.builder(this)
                .withJdbcUrl(url)
                .withProperties(properties)
                .build();
    }

    /**
     * Builder class.
     */
    public static final class Builder {

        private String region;

        private String host;

        private int port;

        private String database;

        private String account;

        private String user;

        private String password;

        private String accessKey;

        private String secretKey;

        private String apiKey;

        private boolean isTableau;

        private boolean isVolcano;

        private Duration queryTimeout;

        private Duration connectTimeout;

        private boolean tcpKeepAlive;

        private boolean tcpNoDelay;

        private boolean secure;

        private String formatCSVDelimiter;

        private boolean skipVerification;

        private boolean enableCompression;

        private Charset charset;

        private long maxBlockSize;

        private String booleanColumnPrefix;

        private boolean insertInfileLocal;

        private Map<SettingKey, Serializable> settings = new HashMap<>();

        private Builder() {
        }

        /**
         * creating a builder class.
         */
        public static Builder builder() {
            return new Builder();
        }

        /**
         * Copy a {@link ByteHouseConfig}.
         */
        public static Builder builder(final ByteHouseConfig cfg) {
            return new Builder()
                    .region(cfg.region())
                    .host(cfg.host())
                    .port(cfg.port())
                    .database(cfg.database())
                    .account(cfg.account())
                    .user(cfg.user())
                    .password(cfg.password())
                    .accessKey(cfg.accessKey())
                    .secretKey(cfg.secretKey())
                    .apiKey(cfg.apiKey())
                    .isTableau(cfg.isTableau())
                    .isVolcano(cfg.isVolcano())
                    .queryTimeout(cfg.queryTimeout())
                    .connectTimeout(cfg.connectTimeout())
                    .tcpKeepAlive(cfg.tcpKeepAlive())
                    .tcpNoDelay(cfg.tcpNoDelay())
                    .secure(cfg.secure())
                    .formatCSVDelimiter(cfg.formatCSVDelimiter())
                    .skipVerification(cfg.skipVerification())
                    .enableCompression(cfg.enableCompression())
                    .charset(cfg.charset())
                    .maxBlockSize(cfg.maxBlockSize())
                    .booleanColumnPrefix(cfg.booleanColumnPrefix())
                    .insertInfileLocal(cfg.insertInfileLocal())
                    .withSettings(cfg.settings());
        }

        public Builder region(String region) {
            this.withSetting(SettingKey.region, region);
            return this;
        }

        public Builder withSetting(final SettingKey key, final Serializable value) {
            this.settings.put(key, value);
            return this;
        }

        public Builder withSettings(final Map<SettingKey, Serializable> settings) {
            CollectionUtil.mergeMapInPlaceKeepLast(this.settings, settings);
            return this;
        }

        public Builder host(final String host) {
            this.withSetting(SettingKey.host, host);
            return this;
        }

        public Builder port(final int port) {
            this.withSetting(SettingKey.port, port);
            return this;
        }

        public Builder database(final String database) {
            this.withSetting(SettingKey.database, database);
            return this;
        }

        public Builder account(final String account) {
            this.withSetting(SettingKey.account, account);
            return this;
        }

        public Builder user(final String user) {
            this.withSetting(SettingKey.user, user);
            return this;
        }

        public Builder password(final String password) {
            this.withSetting(SettingKey.password, password);
            return this;
        }

        public Builder accessKey(final String accessKey) {
            this.withSetting(SettingKey.accessKey, accessKey);
            return this;
        }

        public Builder secretKey(final String secretKey) {
            this.withSetting(SettingKey.secretKey, secretKey);
            return this;
        }

        public Builder apiKey(final String apiKey) {
            this.withSetting(SettingKey.apiKey, apiKey);
            return this;
        }

        public Builder isTableau(final boolean isTableau) {
            this.withSetting(SettingKey.isTableau, isTableau);
            return this;
        }

        public Builder isVolcano(final boolean isVolcano) {
            this.withSetting(SettingKey.isVolcano, isVolcano);
            return this;
        }

        public Builder queryTimeout(final Duration queryTimeout) {
            this.withSetting(SettingKey.queryTimeout, queryTimeout);
            return this;
        }

        public Builder connectTimeout(final Duration connectTimeout) {
            this.withSetting(SettingKey.connectTimeout, connectTimeout);
            return this;
        }

        public Builder tcpKeepAlive(final boolean tcpKeepAlive) {
            this.withSetting(SettingKey.tcpKeepAlive, tcpKeepAlive);
            return this;
        }

        public Builder tcpNoDelay(final boolean tcpNoDelay) {
            this.withSetting(SettingKey.tcpNoDelay, tcpNoDelay);
            return this;
        }

        public Builder secure(final boolean secure) {
            this.withSetting(SettingKey.secure, secure);
            return this;
        }

        public Builder skipVerification(final boolean skipVerification) {
            this.withSetting(SettingKey.skipVerification, skipVerification);
            return this;
        }

        public Builder enableCompression(final boolean enableCompression) {
            this.withSetting(SettingKey.enableCompression, enableCompression);
            return this;
        }

        public Builder insertInfileLocal(final boolean insertInfileLocal) {
            this.withSetting(SettingKey.insertInfileLocal, insertInfileLocal);
            return this;
        }

        public Builder charset(final String charset) {
            this.withSetting(SettingKey.charset, charset);
            return this;
        }

        public Builder charset(final Charset charset) {
            this.withSetting(SettingKey.charset, charset.name());
            return this;
        }

        public Builder maxBlockSize(final long maxBlockSize) {
            this.withSetting(SettingKey.max_block_size, maxBlockSize);
            return this;
        }

        public Builder booleanColumnPrefix(final String booleanColumnPrefix) {
            this.withSetting(SettingKey.booleanColumnPrefix, booleanColumnPrefix);
            return this;
        }

        public Builder formatCSVDelimiter(final String formatCSVDelimiter) {
            this.withSetting(SettingKey.formatCSVDelimiter, formatCSVDelimiter);
            return this;
        }

        public Builder settings(final Map<SettingKey, Serializable> settings) {
            this.settings = settings;
            return this;
        }

        public Builder clearSettings() {
            this.settings = new HashMap<>();
            return this;
        }

        public Builder withJdbcUrl(final String jdbcUrl) {
            return this.withSettings(ByteHouseJdbcUrlParser.parseJdbcUrl(jdbcUrl));
        }

        public Builder withProperties(final Properties properties) {
            return this.withSettings(ByteHouseJdbcUrlParser.parseProperties(properties));
        }

        public ByteHouseConfig build() {
            this.region = (String) this.settings.getOrDefault(SettingKey.region, "");
            handleRegionSettings();

            this.host = (String) this.settings.getOrDefault(SettingKey.host, "127.0.0.1");
            this.port = ((Number) this.settings.getOrDefault(SettingKey.port, 9000)).intValue();
            this.database = (String) this.settings.getOrDefault(SettingKey.database, "");
            this.account = (String) this.settings.getOrDefault(SettingKey.account, "");
            this.user = (String) this.settings.getOrDefault(SettingKey.user, "default");
            this.password = (String) this.settings.getOrDefault(SettingKey.password, "");
            this.accessKey = (String) this.settings.getOrDefault(SettingKey.accessKey, "");
            this.secretKey = (String) this.settings.getOrDefault(SettingKey.secretKey, "");
            this.apiKey = (String) this.settings.getOrDefault(SettingKey.apiKey, "");
            this.isTableau = (boolean) this.settings.getOrDefault(SettingKey.isTableau, false);
            this.isVolcano = (boolean) this.settings.getOrDefault(SettingKey.isVolcano, false);
            this.queryTimeout = (Duration) this.settings.getOrDefault(SettingKey.queryTimeout, Duration.ZERO);
            this.connectTimeout = (Duration) this.settings.getOrDefault(SettingKey.connectTimeout, Duration.ZERO);
            this.tcpKeepAlive = (boolean) this.settings.getOrDefault(SettingKey.tcpKeepAlive, false);
            this.tcpNoDelay = (boolean) this.settings.getOrDefault(SettingKey.tcpNoDelay, true);
            this.secure = (boolean) this.settings.getOrDefault(SettingKey.secure, false);
            this.skipVerification = (boolean) this.settings.getOrDefault(SettingKey.skipVerification, false);
            this.enableCompression = (boolean) this.settings.getOrDefault(SettingKey.enableCompression, false);
            this.charset = Charset.forName((String) this.settings.getOrDefault(SettingKey.charset, "UTF-8"));
            this.maxBlockSize = (long) this.settings.getOrDefault(SettingKey.max_block_size, 65536L);
            this.booleanColumnPrefix = (String) this.settings.getOrDefault(SettingKey.booleanColumnPrefix, "");
            this.insertInfileLocal = (boolean) this.settings.getOrDefault(SettingKey.insertInfileLocal, false);
            this.formatCSVDelimiter = (String) this.settings.getOrDefault(SettingKey.formatCSVDelimiter, ",");

            useDefaultIfNotSet();
            purgeClientSettings();

            return new ByteHouseConfig(
                    region,
                    host,
                    port,
                    database,
                    account,
                    user,
                    password,
                    accessKey,
                    secretKey,
                    apiKey,
                    isTableau,
                    isVolcano,
                    queryTimeout,
                    connectTimeout,
                    tcpKeepAlive,
                    tcpNoDelay,
                    secure,
                    formatCSVDelimiter,
                    skipVerification,
                    enableCompression,
                    charset.name(),
                    maxBlockSize,
                    booleanColumnPrefix,
                    insertInfileLocal,
                    settings
            );
        }

        private void handleRegionSettings() {
            if (StrUtil.isBlank(this.region)) {
                return;
            }
            final ByteHouseRegion bhRegion = ByteHouseRegion.fromString(this.region);
            this.withSetting(SettingKey.host, bhRegion.getHost());
            this.withSetting(SettingKey.port, bhRegion.getPort());
            this.withSetting(SettingKey.secure, true);
        }

        private void useDefaultIfNotSet() {
            if (StrUtil.isBlank(this.host)) this.host = "127.0.0.1";
            if (this.port == -1) this.port = 9000;
            if (StrUtil.isBlank(this.database)) this.database = "";
            if (StrUtil.isBlank(this.account)) this.account = "";
            if (StrUtil.isBlank(this.user)) this.user = "default";
            if (StrUtil.isBlank(this.password)) this.password = "";
            if (this.queryTimeout.isNegative()) this.queryTimeout = Duration.ZERO;
            if (this.connectTimeout.isNegative()) this.connectTimeout = Duration.ZERO;
            if (StrUtil.isBlank(this.booleanColumnPrefix)) this.booleanColumnPrefix = "";
        }

        /**
         * Remove {@link ClientConfigKey} from the {@link ByteHouseConfig#settings}
         * so that they won't be sent to the server.
         */
        private void purgeClientSettings() {
            try {

                final Set<String> bhConfigFields = Arrays
                        .stream(ByteHouseConfig.class.getDeclaredFields())
                        .map(Field::getName)
                        .collect(Collectors.toSet());

                final Field[] declaredFields = SettingKey.class.getDeclaredFields();
                for (final Field declaredField : declaredFields) {
                    if (
                            Modifier.isStatic(declaredField.getModifiers())
                                    && (declaredField.getType() == SettingKey.class)
                                    && declaredField.getAnnotation(ClientConfigKey.class) != null
                    ) {
                        // get static field can pass null.
                        this.settings.remove(declaredField.get(null));

                        if (!bhConfigFields.contains(declaredField.getName())) {
                            throw new InvalidValueException(
                                    String.format("field %s in %s.class does "
                                                    + "not have corresponding key in"
                                                    + " %s.class. This is a programmer error",
                                            declaredField.getName(),
                                            SettingKey.class.getSimpleName(),
                                            ByteHouseConfig.class.getSimpleName()
                                    )
                            );
                        }
                    }
                }
            } catch (IllegalAccessException e) {
                throw new InvalidValueException(e);
            }
        }
    }
}
