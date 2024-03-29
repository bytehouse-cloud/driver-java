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
package com.bytedance.bytehouse.stream;

import com.bytedance.bytehouse.client.ServerContext;
import com.bytedance.bytehouse.data.Block;
import com.bytedance.bytehouse.data.ColumnFactoryUtils;
import com.bytedance.bytehouse.data.DataTypeFactory;
import com.bytedance.bytehouse.data.IColumn;
import com.bytedance.bytehouse.data.IDataType;
import com.bytedance.bytehouse.misc.CheckedIterator;
import com.bytedance.bytehouse.misc.ValidateUtils;
import com.bytedance.bytehouse.protocol.DataResponse;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Support building QueryResult in client side, it's useful for ad-hoc building a ResultSet for JDBC interface,
 * and mock QueryResult for test.
 * <br><br>
 * Note that this class does not execute any query. It simply turns what you give it into a result and
 * give it back to you.
 */
public final class QueryResultBuilder {

    private final int columnNum;

    private final ServerContext serverContext;

    private final List<List<?>> rows = new ArrayList<>();

    private List<String> columnNames;

    private List<IDataType> columnTypes;

    private QueryResultBuilder(final int columnNum, final ServerContext serverContext) {
        this.columnNum = columnNum;
        this.serverContext = serverContext;
    }

    /**
     * Create a builder.
     */
    public static QueryResultBuilder builder(final int columnsNum, final ServerContext serverContext) {
        return new QueryResultBuilder(columnsNum, serverContext);
    }

    /**
     * add a list of column names.
     */
    public QueryResultBuilder columnNames(final String... names) {
        return columnNames(Arrays.asList(names));
    }

    /**
     * add a list of column names.
     * <br><br>
     * throws exception if the length is inconsistent with what's already existing.
     */
    public QueryResultBuilder columnNames(final List<String> names) {
        ValidateUtils.ensure(names.size() == columnNum,
                "size mismatch, req: " + columnNum + " got: " + names.size()
        );
        this.columnNames = names;
        return this;
    }

    /**
     * Add a list of column types described as strings.
     */
    public QueryResultBuilder columnTypes(final String... types) throws SQLException {
        return columnTypes(Arrays.asList(types));
    }

    /**
     * Add a list of column types described as strings.
     * <br><br>
     *
     * @throws com.bytedance.bytehouse.exception.InvalidValueException if the length is inconsistent with what's already existing.
     * @throws SQLException                                            if the string description of the type is not valid.
     */
    public QueryResultBuilder columnTypes(final List<String> types) throws SQLException {
        ValidateUtils.ensure(types.size() == columnNum,
                "size mismatch, req: " + columnNum + " got: " + types.size());
        this.columnTypes = new ArrayList<>(columnNum);
        for (int i = 0; i < columnNum; i++) {
            columnTypes.add(DataTypeFactory.get(types.get(i), serverContext));
        }
        return this;
    }

    /**
     * Add a row of data.
     */
    public QueryResultBuilder addRow(final Object... row) {
        return addRow(Arrays.asList(row));
    }

    /**
     * Add a row of data.
     *
     * @throws com.bytedance.bytehouse.exception.InvalidValueException if the length is inconsistent with what's already existing.
     */
    public QueryResultBuilder addRow(final List<?> row) {
        ValidateUtils.ensure(row.size() == columnNum,
                "size mismatch, req: " + columnNum + " got: " + row.size());
        rows.add(row);
        return this;
    }

    /**
     * build query and return what you have just plugged into this instance as the results.
     */
    public QueryResult build() throws SQLException {
        ValidateUtils.ensure(columnNames != null, "columnNames is null");
        ValidateUtils.ensure(columnTypes != null, "columnTypes is null");

        // assemble header block
        final IColumn[] headerColumns = new IColumn[columnNum];
        final Object[] emptyObjects = new Object[columnNum];
        for (int c = 0; c < columnNum; c++) {
            headerColumns[c] = ColumnFactoryUtils.createColumn(columnNames.get(c), columnTypes.get(c), emptyObjects);
        }
        final Block headerBlock = new Block(0, headerColumns);

        // assemble all rows to one data block
        final IColumn[] dataColumns = new IColumn[columnNum];
        for (int c = 0; c < columnNum; c++) {
            Object[] columnObjects = new Object[rows.size()];
            for (int r = 0; r < rows.size(); r++) {
                columnObjects[r] = rows.get(r).get(c);
            }
            dataColumns[c] = ColumnFactoryUtils.createColumn(columnNames.get(c), columnTypes.get(c), columnObjects);
        }
        final Block dataBlock = new Block(rows.size(), dataColumns);

        return new QueryResult() {
            @Override
            public Block header() throws SQLException {
                return headerBlock;
            }

            @Override
            public CheckedIterator<DataResponse, SQLException> data() {
                final DataResponse data = new DataResponse("client_build", dataBlock);

                return new CheckedIterator<DataResponse, SQLException>() {
                    private final DataResponse dataResponse = data;

                    private boolean beforeFirst = true;

                    public boolean hasNext() {
                        return (beforeFirst);
                    }

                    public DataResponse next() {
                        if (!beforeFirst) {
                            throw new NoSuchElementException();
                        }
                        beforeFirst = false;
                        return dataResponse;
                    }
                };
            }
        };
    }
}
