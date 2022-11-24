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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

class SQLParserUtilsTest {


    @Nested
    static class IsInsertQueryTest {

        @ParameterizedTest(name = "{index} => sql query {0} ")
        @ArgumentsSource(CustomArgumentProvider.class)
        void tests(
                String originalQuery,
                boolean isTrue
        ) {
            assertEquals(SQLParserUtils.isInsertQuery(originalQuery), isTrue);
        }

        static class CustomArgumentProvider implements ArgumentsProvider {

            @Override
            public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
                return Stream.of(
                        Arguments.of(
                                "INSERT INTO `flink_cnch_connector_test` (`a`, `b`, `c`, `d`) FORMAT Values SETTINGS insertion_label = 'testLabel' (?, ?, ?, ?)",
                                true
                        ),
                        Arguments.of(
                                "select * from `dataexpress`.`npc_cases`",
                                false
                        ),
                        Arguments.of(
                                "describe table `dataexpress`.`npc_cases`",
                                false
                        ),
                        Arguments.of(
                                "insert into table (col1, col2) values ('a','b')",
                                true
                        ),
                        Arguments.of(
                                "INSERT INTO inventory.orders  FORMAT Values SETTINGS insertion_label = 'xxx'  ('54895','Apple',12)",
                                true
                        )
                );
            }
        }
    }

    @Nested
    static class SplitInsertQueryTest {

        @ParameterizedTest(name = "{index} => sql query {0} ")
        @ArgumentsSource(CustomArgumentProvider.class)
        void tests(
                String originalQuery,
                String queryPart,
                String valuePart,
                Class<IllegalArgumentException> exceptionType
        ) {
            if (exceptionType == null) {
                final SQLParserUtils.InsertQueryParts parts = SQLParserUtils.splitInsertQuery(originalQuery);
                assertEquals(parts.queryPart, queryPart);
                assertEquals(parts.valuePart, valuePart);
            } else {
                assertThrows(exceptionType, () -> {
                    SQLParserUtils.splitInsertQuery(originalQuery);
                });
            }
        }

        static class CustomArgumentProvider implements ArgumentsProvider {

            @Override
            public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
                return Stream.of(
                        Arguments.of(
                                "insert into table (col1, col2) values ('a','b')",
                                "insert into table (col1, col2) values",
                                "('a','b')",
                                null
                        ),
                        Arguments.of(
                                "insert into table (col1, col2) VALUES(?,?)",
                                "insert into table (col1, col2) VALUES",
                                "(?,?)",
                                null
                        ),
                        Arguments.of(
                                "insert into table (col1, col2) FORMAT Values SETTINGS insertion_label = 'xxx' ('a','b')",
                                "insert into table (col1, col2) FORMAT Values SETTINGS insertion_label = 'xxx'",
                                "('a','b')",
                                null
                        ),
                        Arguments.of(
                                "insert into table (col1, col2) FORMAT Values SETTINGS preload_checksums_and_primary_index_cache = 0, max_threads_for_cnch_dump = 16 ('a','b')",
                                "insert into table (col1, col2) FORMAT Values SETTINGS preload_checksums_and_primary_index_cache = 0, max_threads_for_cnch_dump = 16",
                                "('a','b')",
                                null
                        ),
                        Arguments.of(
                                "insert into table (col1, col2) FORMAT Values SETTINGS insertion_label='xxx' ('a','b')",
                                "insert into table (col1, col2) FORMAT Values SETTINGS insertion_label='xxx'",
                                "('a','b')",
                                null
                        ),
                        Arguments.of(
                                "insert into table (col1, col2) FORMAT Values SETTINGS insertion_label='(xxx)' ('a','b')",
                                "insert into table (col1, col2) FORMAT Values SETTINGS insertion_label='(xxx)'",
                                "('a','b')",
                                null
                        ),
                        Arguments.of(
                                "insert into table (col1, col2) FORMAT Values SETTINGS insertion_label = '_' ('a','b')",
                                "",
                                "",
                                IllegalArgumentException.class
                        ),
                        Arguments.of(
                                "insert into table (col1, col2) FORMAT Values SETTINGS insertion_label = `xxx` ('a','b')",
                                "",
                                "",
                                IllegalArgumentException.class
                        )
                );
            }
        }
    }

    @Nested
    static class ExtractDBAndTableNameTest {

        private final String defaultDb = "defaultDb";

        @ParameterizedTest(name = "{index} => sql query {0} ")
        @ArgumentsSource(CustomArgumentProvider.class)
        void tests(
                String query
        ) {
            final SQLParserUtils.DbTable dbTable = SQLParserUtils
                    .extractDBAndTableName(query);

            final OriginalImpl originalImpl = new OriginalImpl(defaultDb).extractDBAndTableName(query);
            assertEquals(dbTable.getDbOrDefault(defaultDb), originalImpl.db);
            assertEquals(dbTable.getTable(), originalImpl.table);
        }

        static class CustomArgumentProvider implements ArgumentsProvider {

            @Override
            public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
                return Stream.of(
                        Arguments.of(
                                "select * from `dataexpress`.`npc_cases`"
                        ),
                        Arguments.of(
                                "insert into table (col1, col2) values ('a','b')"
                        ),
                        Arguments.of(
                                "describe table `dataexpress`.`npc_cases`"
                        )
                );
            }
        }
    }

    /**
     * This is the original implementation from the open source. You can check the output.
     * the output is actually a bit weird. I don't understand the design yet hence I prefer
     * to keep it unchanged. Therefore, the tests written are checked against the original
     * implementation to make sure they are the same.
     */
    private static class OriginalImpl {

        private static final Pattern SELECT_DB_TABLE = Pattern.compile("(?i)FROM\\s+(\\S+\\.)?(\\S+)");

        public String db;

        public String table = "unknown";

        public OriginalImpl(final String db) {
            this.db = db;
        }

        private OriginalImpl extractDBAndTableName(final String sql) {
            final String upperSQL = sql.trim().toUpperCase(Locale.ROOT);
            if (upperSQL.startsWith("SELECT")) {
                final Matcher m = SELECT_DB_TABLE.matcher(sql);
                if (m.find()) {
                    if (m.groupCount() == 2) {
                        if (m.group(1) != null) {
                            db = m.group(1);
                        }
                        table = m.group(2);
                    }
                }
            } else if (upperSQL.startsWith("DESC")) {
                db = "system";
                table = "columns";
            } else if (upperSQL.startsWith("SHOW")) {
                db = "system";
                table = upperSQL.contains("TABLES") ? "tables" : "databases";
            }

            return this;
        }
    }
}
