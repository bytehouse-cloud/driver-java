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
import com.bytedance.bytehouse.log.Logger;
import com.bytedance.bytehouse.log.LoggerFactory;
import com.bytedance.bytehouse.misc.Validate;
import com.bytedance.bytehouse.settings.SettingKey;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class parse JDBC url to validate if they are correct.
 */
public final class ByteHouseJdbcUrlParser {

    public static final String JDBC_PREFIX = "jdbc:";

    public static final String BYTEHOUSE_PREFIX = "bytehouse:";

    public static final String JDBC_BYTEHOUSE_PREFIX = JDBC_PREFIX + BYTEHOUSE_PREFIX;

    public static final String CNCH_PREFIX = "cnch:";

    public static final String JDBC_CNCH_PREFIX = JDBC_PREFIX + CNCH_PREFIX;

    public static final Pattern DB_PATH_PATTERN = Pattern.compile("/([a-zA-Z0-9_]+)");

    public static final Pattern HOST_PORT_PATH_PATTERN =
            Pattern.compile("//(?<host>[^/:\\s]+)(:(?<port>\\d+))?");

    private static final Logger LOG = LoggerFactory.getLogger(ByteHouseJdbcUrlParser.class);

    /**
     * Parse and extract jdbcUrl.
     */
    public static Map<SettingKey, Serializable> parseJdbcUrl(final String jdbcUrl) {
        try {
            final URI uri = new URI(jdbcUrl.substring(JDBC_PREFIX.length()));
            final String host = uri.getHost();
            final Integer port = uri.getPort();
            String database = uri.getPath() == null ? "" : uri.getPath();
            database = database.startsWith("/") ? database.substring(1) : database;

            final String scheme = uri.getScheme();
            final Map<SettingKey, Serializable> settings = new HashMap<>();
            settings.put(SettingKey.isCnch,
                    scheme != null && scheme.toLowerCase(Locale.ROOT).equals("cnch")
            );
            settings.put(SettingKey.host, host);
            settings.put(SettingKey.port, port);
            settings.put(SettingKey.database, database);
            settings.putAll(extractQueryParameters(uri.getQuery()));

            return settings;
        } catch (URISyntaxException ex) {
            throw new InvalidValueException(ex);
        }
    }

    /**
     * Parse and extract jdbc properties.
     */
    public static Map<SettingKey, Serializable> parseProperties(final Properties properties) {
        final Map<SettingKey, Serializable> settings = new HashMap<>();

        for (final String name : properties.stringPropertyNames()) {
            final String value = properties.getProperty(name);

            parseSetting(settings, name, value);
        }

        return settings;
    }

    private static String parseDatabase(final String jdbcUrl) throws URISyntaxException {
        final URI uri = new URI(jdbcUrl.substring(JDBC_PREFIX.length()));
        String database = uri.getPath();
        if (database != null && !database.isEmpty()) {
            Matcher m = DB_PATH_PATTERN.matcher(database);
            if (m.matches()) {
                database = m.group(1);
            } else {
                throw new URISyntaxException(
                        String.format("wrong database name path: '%s'", database),
                        jdbcUrl
                );
            }
        }
        if (database != null && database.isEmpty()) {
            database = "";
        }
        return database;
    }

    private static String parseHost(final String jdbcUrl) throws URISyntaxException {
        final String uriStr = jdbcUrl.substring(JDBC_PREFIX.length());
        final URI uri = new URI(uriStr);
        String host = uri.getHost();
        if (host == null || host.isEmpty()) {
            Matcher m = HOST_PORT_PATH_PATTERN.matcher(uriStr);
            if (m.find()) {
                host = m.group("host");
            } else {
                throw new URISyntaxException("No valid host was found", jdbcUrl);
            }
        }
        return host;
    }

    private static int parsePort(final String jdbcUrl) {
        final String uriStr = jdbcUrl.substring(JDBC_PREFIX.length());
        URI uri;
        try {
            uri = new URI(uriStr);
        } catch (Exception ex) {
            throw new InvalidValueException(ex);
        }
        int port = uri.getPort();
        if (port <= -1) {
            Matcher m = HOST_PORT_PATH_PATTERN.matcher(uriStr);
            if (m.find() && m.group("port") != null) {
                port = Integer.parseInt(m.group("port"));
            }
        }
        if (port == 8123) {
            LOG.warn("8123 is default HTTP port, you may connect with error protocol!");
        }
        return port;
    }

    private static Map<SettingKey, Serializable> extractQueryParameters(
            final String queryParameters
    ) {
        final Map<SettingKey, Serializable> parameters = new HashMap<>();
        final StringTokenizer tokenizer = new StringTokenizer(
                queryParameters == null ? "" : queryParameters, "&"
        );

        while (tokenizer.hasMoreTokens()) {
            final String[] queryParameter = tokenizer.nextToken().split("=", 2);
            Validate.ensure(queryParameter.length == 2,
                    String.format("ByteHouse JDBC URL Parameter '%s' Error, "
                            + "Expected '='.", queryParameters)
            );

            final String name = queryParameter[0];
            final String value = queryParameter[1];

            parseSetting(parameters, name, value);
        }
        return parameters;
    }

    private static void parseSetting(
            final Map<SettingKey, Serializable> settings,
            final String name,
            final String value
    ) {
        final SettingKey settingKey = SettingKey.definedSettingKeys()
                .get(name.toLowerCase(Locale.ROOT));
        if (settingKey != null) {
            settings.put(settingKey, settingKey.type().deserializeURL(value));
        } else {
            LOG.warn("ignore undefined setting: {}={}", name, value);
        }
    }
}
