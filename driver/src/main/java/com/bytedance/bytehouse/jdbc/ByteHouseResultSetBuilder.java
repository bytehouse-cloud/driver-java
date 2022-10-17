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

import com.bytedance.bytehouse.client.ServerContext;
import com.bytedance.bytehouse.misc.ValidateUtils;
import com.bytedance.bytehouse.settings.BHConstants;
import com.bytedance.bytehouse.settings.ByteHouseConfig;
import com.bytedance.bytehouse.stream.QueryResult;
import com.bytedance.bytehouse.stream.QueryResultBuilder;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

/**
 * This class builds {@link QueryResult} into a {@link ByteHouseResultSet}.
 */
public final class ByteHouseResultSetBuilder {

    private final QueryResultBuilder queryResultBuilder;

    private ByteHouseConfig cfg;

    private String db = BHConstants.DEFAULT_DATABASE;

    private String table = "unknown";

    private ByteHouseResultSetBuilder(
            final QueryResultBuilder queryResultBuilder
    ) {
        this.queryResultBuilder = queryResultBuilder;
    }

    /**
     * factory method to create a builder.
     */
    public static ByteHouseResultSetBuilder builder(
            final int columnsNum,
            final ServerContext serverContext
    ) {
        return new ByteHouseResultSetBuilder(QueryResultBuilder.builder(columnsNum, serverContext));
    }

    /**
     * set the {@link ByteHouseConfig}.
     *
     * @param cfg
     * @return
     */
    public ByteHouseResultSetBuilder cfg(final ByteHouseConfig cfg) {
        this.cfg = cfg;
        return this;
    }

    /**
     * set database name.
     */
    public ByteHouseResultSetBuilder db(final String db) {
        this.db = db;
        return this;
    }

    /**
     * set table name.
     */
    public ByteHouseResultSetBuilder table(final String table) {
        this.table = table;
        return this;
    }

    /**
     * set column names.
     */
    public ByteHouseResultSetBuilder columnNames(final String... names) {
        return columnNames(Arrays.asList(names));
    }

    /**
     * set column names.
     */
    public ByteHouseResultSetBuilder columnNames(final List<String> names) {
        this.queryResultBuilder.columnNames(names);
        return this;
    }

    /**
     * set column types.
     */
    public ByteHouseResultSetBuilder columnTypes(final String... types) throws SQLException {
        return columnTypes(Arrays.asList(types));
    }

    /**
     * set column types.
     */
    public ByteHouseResultSetBuilder columnTypes(final List<String> types) throws SQLException {
        this.queryResultBuilder.columnTypes(types);
        return this;
    }

    /**
     * add a row of data.
     */
    public ByteHouseResultSetBuilder addRow(final Object... row) {
        return addRow(Arrays.asList(row));
    }

    /**
     * add a row of data.
     */
    public ByteHouseResultSetBuilder addRow(final List<?> row) {
        this.queryResultBuilder.addRow(row);
        return this;
    }

    /**
     * build {@link ByteHouseResultSet} from the builder.
     */
    public ByteHouseResultSet build() throws SQLException {
        ValidateUtils.ensure(cfg != null);
        final QueryResult queryResult = this.queryResultBuilder.build();
        return new ByteHouseResultSet(
                null,
                cfg,
                db,
                table,
                queryResult.header(),
                queryResult.data()
        );
    }
}
