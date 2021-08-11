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
package com.bytedance.bytehouse.jdbc.statement;

import com.bytedance.bytehouse.client.NativeContext;
import com.bytedance.bytehouse.data.Block;
import com.bytedance.bytehouse.data.DataTypeConverter;
import com.bytedance.bytehouse.data.IColumn;
import com.bytedance.bytehouse.jdbc.ByteHouseConnection;
import com.bytedance.bytehouse.log.Logger;
import com.bytedance.bytehouse.log.LoggerFactory;
import com.bytedance.bytehouse.misc.ExceptionUtil;
import com.bytedance.bytehouse.misc.Validate;
import com.bytedance.bytehouse.stream.ValuesWithParametersNativeInputFormat;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;

public class ByteHousePreparedInsertStatement extends AbstractPreparedStatement {

    private static final Logger LOG = LoggerFactory.getLogger(ByteHousePreparedInsertStatement.class);

    private final int posOfData;

    private final String fullQuery;

    private final String insertQuery;

    private boolean blockInit;

    private final DataTypeConverter dataTypeConverter;

    public ByteHousePreparedInsertStatement(int posOfData,
                                            String fullQuery,
                                            ByteHouseConnection conn,
                                            NativeContext nativeContext) throws SQLException {
        super(conn, nativeContext, null);
        this.blockInit = false;
        this.posOfData = posOfData;
        this.fullQuery = fullQuery;
        this.insertQuery = fullQuery.substring(0, posOfData);
        this.dataTypeConverter = new DataTypeConverter(tz);

        initBlockIfPossible();
    }

    private static int computeQuestionMarkSize(String query, int start) throws SQLException {
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
                    Validate.isTrue(i > start, "");
                    param++;
                }
            }
        }
        return param;
    }

    // paramPosition start with 1
    @Override
    public void setObject(int paramPosition, Object x) throws SQLException {
        initBlockIfPossible();
        int columnIdx = block.paramIdx2ColumnIdx(paramPosition - 1);
        IColumn column = block.getColumn(columnIdx);
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
    }

    @Override
    public void clearBatch() throws SQLException {
    }

    @Override
    public int[] executeBatch() throws SQLException {
        int rows = creator.sendInsertRequest(block);
        int[] result = new int[rows];
        Arrays.fill(result, 1);
        clearBatch();
        this.blockInit = false;
        this.block.initWriteBuffer();
        return result;
    }

    @Override
    public void close() throws SQLException {
        if (blockInit) {
            // Empty insert when close.
            this.creator.sendInsertRequest(new Block());
            this.blockInit = false;
            this.block.initWriteBuffer();
        }
        super.close();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(super.toString());
        sb.append(": ");
        try {
            sb.append(insertQuery).append(" (");
            for (int i = 0; i < block.columnCnt(); i++) {
                Object obj = block.getObject(i);
                if (obj == null) {
                    sb.append("?");
                } else if (obj instanceof Number) {
                    sb.append(obj);
                } else {
                    sb.append("'").append(obj).append("'");
                }
                if (i < block.columnCnt() - 1) {
                    sb.append(",");
                }
            }
            sb.append(")");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return sb.toString();
    }

    private void initBlockIfPossible() throws SQLException {
        if (this.blockInit) {
            return;
        }
        ExceptionUtil.rethrowSQLException(() -> {
            this.block = creator.getSampleBlock(insertQuery);
            this.block.initWriteBuffer();
            this.blockInit = true;
            new ValuesWithParametersNativeInputFormat(posOfData, fullQuery).fill(block);
        });
    }

    private void addParameters() throws SQLException {
        block.appendRow();
    }
}
