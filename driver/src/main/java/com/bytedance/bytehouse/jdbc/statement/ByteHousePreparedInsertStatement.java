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
package com.bytedance.bytehouse.jdbc.statement;

import com.bytedance.bytehouse.client.ServerContext;
import com.bytedance.bytehouse.data.DataTypeConverter;
import com.bytedance.bytehouse.data.IColumn;
import com.bytedance.bytehouse.exception.ByteHouseClientException;
import com.bytedance.bytehouse.jdbc.ByteHouseConnection;
import com.bytedance.bytehouse.jdbc.ByteHouseResultSet;
import com.bytedance.bytehouse.log.Logger;
import com.bytedance.bytehouse.log.LoggerFactoryUtils;
import com.bytedance.bytehouse.misc.ExceptionUtil;
import com.bytedance.bytehouse.misc.InfileCSVReaderUtils;
import com.bytedance.bytehouse.misc.SQLParserUtils;
import com.bytedance.bytehouse.stream.ValuesWithParametersNativeInputFormat;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

public class ByteHousePreparedInsertStatement extends AbstractPreparedStatement {

    private static final Logger LOG = LoggerFactoryUtils.getLogger(ByteHousePreparedInsertStatement.class);

    private final String insertQueryPart;

    private final String valuePart;

    private final DataTypeConverter dataTypeConverter;

    private boolean blockInit;

    private int rowInsertedCount;

    public ByteHousePreparedInsertStatement(
            final String insertQueryPart,
            final String valuePart,
            final ByteHouseConnection conn,
            final ServerContext serverContext
    ) throws SQLException {
        super(conn, serverContext, null);
        this.blockInit = false;
        this.insertQueryPart = insertQueryPart;
        this.valuePart = valuePart;
        this.dataTypeConverter = new DataTypeConverter(tz);
        this.rowInsertedCount = 0;

        initBlockIfPossible();
        final SQLParserUtils.DbTable dbTable = SQLParserUtils.extractDBAndTableName(insertQueryPart);
        lastResultSet = new ByteHouseResultSet(this, cfg, dbTable.getDbOrDefault(this.defaultDb), dbTable.getTable(), this.block, null);
    }

    // paramPosition start with 1
    @Override
    public void setObject(final int paramPosition, final Object x) throws SQLException {
        initBlockIfPossible();
        final int columnIdx = block.paramIdx2ColumnIdx(paramPosition - 1);
        final IColumn column = block.getColumn(columnIdx);
        block.setObject(columnIdx, dataTypeConverter.convertJdbcToJava(column.type(), x));
    }

    @Override
    public boolean execute() throws SQLException {
        return executeQuery() != null;
    }

    @Override
    public int executeUpdate() throws SQLException {
        addParameters();
        int result = creator.sendInsertRequest(block);
        this.blockInit = false;
        this.block.initWriteBuffer();
        return result;
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        executeUpdate();
        return null;
    }

    @Override
    public void addBatch() throws SQLException {
        addParameters();
        executeBatchIfReachMaxSize();
    }

    private void executeBatchIfReachMaxSize() throws SQLException {
        if (block.rowCnt() < cfg.maxBlockSize()) {
            return;
        }
        rowInsertedCount += creator.sendBlock(block);
        block.reuseBlock();
    }

    public int executeCSVBlock(InfileCSVReaderUtils.CSVBlock csvBlock, boolean hasHeader) throws SQLException {
        for (List<String> recordList: csvBlock.getRows()) {
            for (int i=0; i<recordList.size(); i++) {
                int headerIndex = hasHeader ? this.block.getPositionByName(csvBlock.getHeaders().get(i)) : (i+1);
                this.setObject(headerIndex, recordList.get(i));
            }
            this.addBatch();
        }
        this.executeBatch();
        return csvBlock.getRowCount();
    }

    @Override
    public void clearBatch() throws SQLException {
    }

    @Override
    public int[] executeBatch() throws SQLException {
        rowInsertedCount += creator.sendInsertRequest(block);
        final int[] result = new int[rowInsertedCount];
        Arrays.fill(result, 1);
        clearBatch();
        this.blockInit = false;
        this.block.initWriteBuffer();
        this.rowInsertedCount = 0;
        return result;
    }

    @Override
    public void close() throws SQLException {
        if (blockInit) {
            this.blockInit = false;
            this.block.initWriteBuffer();
        }
        super.close();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append(super.toString()).append(": ");
        try {
            sb.append(insertQueryPart).append(" (");
            for (int i = 0; i < block.columnCnt(); i++) {
                final Object obj = block.getObject(i);
                if (obj == null) {
                    sb.append('?');
                } else if (obj instanceof Number) {
                    sb.append(obj);
                } else {
                    sb.append('\'').append(obj).append('\'');
                }
                if (i < block.columnCnt() - 1) {
                    sb.append(',');
                }
            }
            sb.append(')');
        } catch (Exception e) {
            throw new ByteHouseClientException(e);
        }
        return sb.toString();
    }

    private void initBlockIfPossible() throws SQLException {
        if (this.blockInit) {
            return;
        }
        ExceptionUtil.rethrowSQLException(() -> {
            String queryId = consumeQueryId();
            this.block = creator.getSampleBlock(queryId, insertQueryPart);
            this.block.initWriteBuffer();
            this.blockInit = true;
            new ValuesWithParametersNativeInputFormat(0, valuePart).fill(block);
        });
    }

    private void addParameters() throws SQLException {
        block.appendRow();
    }
}
