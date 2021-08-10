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

import com.bytedance.bytehouse.client.NativeContext;
import com.bytedance.bytehouse.misc.Validate;
import com.bytedance.bytehouse.settings.BHConstants;
import com.bytedance.bytehouse.settings.ByteHouseConfig;
import com.bytedance.bytehouse.stream.QueryResult;
import com.bytedance.bytehouse.stream.QueryResultBuilder;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

public class ByteHouseResultSetBuilder {

    private final QueryResultBuilder queryResultBuilder;

    private ByteHouseConfig cfg;

    private String db = BHConstants.DEFAULT_DATABASE;

    private String table = "unknown";

    private ByteHouseResultSetBuilder(QueryResultBuilder queryResultBuilder) {
        this.queryResultBuilder = queryResultBuilder;
    }

    public static ByteHouseResultSetBuilder builder(int columnsNum, NativeContext.ServerContext serverContext) {
        return new ByteHouseResultSetBuilder(QueryResultBuilder.builder(columnsNum, serverContext));
    }

    public ByteHouseResultSetBuilder cfg(ByteHouseConfig cfg) {
        this.cfg = cfg;
        return this;
    }

    public ByteHouseResultSetBuilder db(String db) {
        this.db = db;
        return this;
    }

    public ByteHouseResultSetBuilder table(String table) {
        this.table = table;
        return this;
    }

    public ByteHouseResultSetBuilder columnNames(String... names) {
        return columnNames(Arrays.asList(names));
    }

    public ByteHouseResultSetBuilder columnNames(List<String> names) {
        this.queryResultBuilder.columnNames(names);
        return this;
    }

    public ByteHouseResultSetBuilder columnTypes(String... types) throws SQLException {
        return columnTypes(Arrays.asList(types));
    }

    public ByteHouseResultSetBuilder columnTypes(List<String> types) throws SQLException {
        this.queryResultBuilder.columnTypes(types);
        return this;
    }

    public ByteHouseResultSetBuilder addRow(Object... row) {
        return addRow(Arrays.asList(row));
    }

    public ByteHouseResultSetBuilder addRow(List<?> row) {
        this.queryResultBuilder.addRow(row);
        return this;
    }

    public ByteHouseResultSet build() throws SQLException {
        Validate.ensure(cfg != null);
        QueryResult queryResult = this.queryResultBuilder.build();
        return new ByteHouseResultSet(null, cfg, db, table, queryResult.header(), queryResult.data());
    }
}
