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
package com.bytedance.bytehouse.misc;

import java.sql.SQLException;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.concurrent.Immutable;

/**
 * provides some easy utility methods to parse a sql statement.
 */
public final class SQLParserUtils {

    static final Pattern TXN_LABEL_BRACKET_REGEX = Pattern
            .compile("insertion_label\\s*=\\s*'[a-zA-Z0-9\\-#()]+'\\s*");

    private static final Pattern GENERIC_INSERT_VALUES_BRACKET_REGEX = Pattern
            .compile(
                    "((?i)VALUES(?-i)\\s*\\()|((?i)FORMAT VALUES SETTINGS(?-i)\\s+([a-zA-Z_]\\w*|`[a-zA-Z_]\\w*`)"
                    + "\\s*=\\s*(\\S+|'.+')(\\s*,\\s*([a-zA-Z_]\\w*|`[a-zA-Z_]\\w*`)\\s*=\\s*(\\S+|'.+'))*\\s+\\()"
            );

    private static final Pattern VALUES_REGEX = Pattern
            .compile("[V|v][A|a][L|l][U|u][E|e][S|s]");

    private static final Pattern SELECT_DB_TABLE = Pattern.compile("(?i)FROM\\s+(\\S+\\.)?(\\S+)");

    private SQLParserUtils() {
        // no creation
    }

    /**
     * splits a insert query into 2 parts:<br>
     * 1. query part: the part to send to the server to get a sample block. <br>
     * 2. value part: the part that contains the insertion values. <br>
     */
    public static InsertQueryParts splitInsertQuery(final String query) {
        if (query.contains("insertion_label") && !TXN_LABEL_BRACKET_REGEX.matcher(query).find()) {
            throw new IllegalArgumentException(
                    "invalid syntax for labelled transaction: " + query);
        }
        final Matcher matcher = GENERIC_INSERT_VALUES_BRACKET_REGEX.matcher(query);
        if (matcher.find()) {
            final int idxBracket = matcher.end() - 1;
            return new InsertQueryParts(
                    query.substring(0, idxBracket).trim(),
                    query.substring(idxBracket).trim()
            );
        } else {
            throw new IllegalArgumentException(
                    "invalid syntax for insert query: " + query);
        }
    }

    public static boolean isInsertQuery(final String query) {
        final Matcher matcher = VALUES_REGEX.matcher(query);
        return matcher.find() && query.trim().toUpperCase(Locale.ROOT).startsWith("INSERT");
    }

    public static DbTable extractDBAndTableName(final String sql) {
        final String upperSQL = sql.trim().toUpperCase(Locale.ROOT);
        if (upperSQL.startsWith("SELECT")) {
            final Matcher m = SELECT_DB_TABLE.matcher(sql);
            if (m.find() && m.groupCount() == 2) {
                String db = null;
                if (m.group(1) != null) {
                    db = m.group(1);
                }
                final String table = m.group(2);
                return new DbTable(db, table);
            }
        } else if (upperSQL.startsWith("DESC")) {
            return new DbTable("system", "columns");
        } else if (upperSQL.startsWith("SHOW")) {
            return new DbTable("system", upperSQL.contains("TABLES") ? "tables" : "databases");
        }
        return new DbTable(null, null);
    }

    /**
     * ported from original implementation. it was not used previously but kept. Hence
     * I just pull it out here and keep it as well.
     */
    public static int computeQuestionMarkSize(
            final String query,
            final int start
    ) throws SQLException {
        int param = 0;
        boolean inQuotes = false, inBackQuotes = false;
        for (int i = 0; i < query.length(); i++) {
            char ch = query.charAt(i);
            if (ch == '`') {
                inBackQuotes = !inBackQuotes;
            } else if (ch == '\'') {
                inQuotes = !inQuotes;
            } else if (!inBackQuotes && !inQuotes) {
                if (ch == '?') {
                    ValidateUtils.isTrue(i > start, "");
                    param++;
                }
            }
        }
        return param;
    }

    @Immutable
    public static class DbTable {

        private final String db;

        private final String table;

        public DbTable(final String db, final String table) {
            this.db = db;
            this.table = table;
        }

        public String getDbOrDefault(final String defaultValue) {
            if (db == null) return defaultValue;
            else return db;
        }

        public String getTable() {
            return table == null ? "unknown" : table;
        }

        @Override
        public String toString() {
            return "DbTable{" +
                    "db='" + db + '\'' +
                    ", table='" + table + '\'' +
                    '}';
        }
    }

    @Immutable
    public static class InsertQueryParts {

        public final String queryPart;

        public final String valuePart;

        public InsertQueryParts(final String queryPart, final String valuePart) {
            this.queryPart = queryPart;
            this.valuePart = valuePart;
        }

        @Override
        public String toString() {
            return "InsertQueryParts{" +
                    "queryPart='" + queryPart + '\'' +
                    ", valuePart='" + valuePart + '\'' +
                    '}';
        }
    }
}
