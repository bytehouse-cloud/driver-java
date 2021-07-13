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

import com.bytedance.bytehouse.data.Block;
import com.bytedance.bytehouse.data.IColumn;
import com.bytedance.bytehouse.data.IDataType;
import com.bytedance.bytehouse.data.type.*;
import com.bytedance.bytehouse.data.type.complex.*;
import com.bytedance.bytehouse.jdbc.ByteHouseArray;
import com.bytedance.bytehouse.log.Logger;
import com.bytedance.bytehouse.log.LoggerFactory;
import com.bytedance.bytehouse.client.NativeContext;
import com.bytedance.bytehouse.exception.ByteHouseSQLException;
import com.bytedance.bytehouse.jdbc.ByteHouseConnection;
import com.bytedance.bytehouse.jdbc.ByteHouseStruct;
import com.bytedance.bytehouse.misc.BytesCharSeq;
import com.bytedance.bytehouse.misc.DateTimeUtil;
import com.bytedance.bytehouse.misc.ExceptionUtil;
import com.bytedance.bytehouse.misc.Validate;
import com.bytedance.bytehouse.stream.ValuesWithParametersNativeInputFormat;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Inet6Address;
import java.net.UnknownHostException;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.UUID;

import static com.bytedance.bytehouse.misc.ExceptionUtil.unchecked;

public class ByteHousePreparedInsertStatement extends AbstractPreparedStatement {

    private static final Logger LOG = LoggerFactory.getLogger(ByteHousePreparedInsertStatement.class);

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

    private final int posOfData;
    private final String fullQuery;
    private final String insertQuery;
    private boolean blockInit;

    public ByteHousePreparedInsertStatement(int posOfData,
                                            String fullQuery,
                                            ByteHouseConnection conn,
                                            NativeContext nativeContext) throws SQLException {
        super(conn, nativeContext, null);
        this.blockInit = false;
        this.posOfData = posOfData;
        this.fullQuery = fullQuery;
        this.insertQuery = fullQuery.substring(0, posOfData);

        initBlockIfPossible();
    }

    // paramPosition start with 1
    @Override
    public void setObject(int paramPosition, Object x) throws SQLException {
        initBlockIfPossible();
        int columnIdx = block.paramIdx2ColumnIdx(paramPosition - 1);
        IColumn column = block.getColumn(columnIdx);
        block.setObject(columnIdx, convertToCkDataType(column.type(), x));
    }

    @Override
    public boolean execute() throws SQLException {
        return executeQuery() != null;
    }

    @Override
    public int executeUpdate() throws SQLException {
        addParameters();
        int result = connection.sendInsertRequest(block);
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
        int rows = connection.sendInsertRequest(block);
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
            this.connection.sendInsertRequest(new Block());
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
            this.block = connection.getSampleBlock(insertQuery);
            this.block.initWriteBuffer();
            this.blockInit = true;
            new ValuesWithParametersNativeInputFormat(posOfData, fullQuery).fill(block);
        });
    }

    private void addParameters() throws SQLException {
        block.appendRow();
    }

    // TODO we actually need a type cast system rather than put all type cast stuffs here
    // TODO create type cast system between Java type (internal representation of CNCH type) and JDBC type
    private Object convertToCkDataType(IDataType<?, ?> type, Object obj) throws ByteHouseSQLException {
        if (obj == null) {
            if (type.nullable() || type instanceof DataTypeNothing)
                return null;
            throw new ByteHouseSQLException(-1, "type[" + type.name() + "] doesn't support null value");
        }
        // put the most common cast at first to avoid `instanceof` test overhead
        if (type instanceof DataTypeString || type instanceof DataTypeFixedString) {
            if (obj instanceof CharSequence)
                return obj;
            if (obj instanceof byte[])
                return new BytesCharSeq((byte[]) obj);
            String objStr = obj.toString();
            LOG.debug("set value[{}]: {} on String Column", obj.getClass(), obj);
            return objStr;
        }
        if (type instanceof DataTypeDate) {
            if (obj instanceof java.util.Date)
                return ((Date) obj).toLocalDate();
            if (obj instanceof LocalDate)
                return obj;
        }
        // TODO support
        //   1. other Java8 time, i.e. OffsetDateTime, Instant
        //   2. unix timestamp, but in second or millisecond?
        if (type instanceof DataTypeDateTime || type instanceof DataTypeDateTime64) {
            if (obj instanceof Timestamp)
                return DateTimeUtil.toZonedDateTime((Timestamp) obj, tz);
            if (obj instanceof LocalDateTime)
                return ((LocalDateTime) obj).atZone(tz);
            if (obj instanceof ZonedDateTime)
                return obj;
        }
        if (type instanceof DataTypeInt8) {
            if (obj instanceof Number)
                return ((Number) obj).byteValue();
        }
        if (type instanceof DataTypeUInt8 || type instanceof DataTypeInt16) {
            if (obj instanceof Number)
                return ((Number) obj).shortValue();
        }
        if (type instanceof DataTypeUInt16 || type instanceof DataTypeInt32) {
            if (obj instanceof Number)
                return ((Number) obj).intValue();
        }
        if (type instanceof DataTypeUInt32 || type instanceof DataTypeInt64) {
            if (obj instanceof Number)
                return ((Number) obj).longValue();
        }
        if (type instanceof DataTypeUInt64) {
            if (obj instanceof BigInteger)
                return obj;
            if (obj instanceof BigDecimal)
                return ((BigDecimal) obj).toBigInteger();
            if (obj instanceof Number)
                return BigInteger.valueOf(((Number) obj).longValue());
        }
        if (type instanceof DataTypeFloat32) {
            if (obj instanceof Number)
                return ((Number) obj).floatValue();
        }
        if (type instanceof DataTypeFloat64) {
            if (obj instanceof Number)
                return ((Number) obj).doubleValue();
        }
        if (type instanceof DataTypeDecimal) {
            if (obj instanceof BigDecimal)
                return obj;
            if (obj instanceof BigInteger)
                return new BigDecimal((BigInteger) obj);
            if (obj instanceof Number)
                return ((Number) obj).doubleValue();
        }
        if (type instanceof DataTypeUUID) {
            if (obj instanceof UUID)
                return obj;
            if (obj instanceof String) {
                return UUID.fromString((String) obj);
            }
        }
        if (type instanceof DataTypeIPv6) {
            if (obj instanceof Inet6Address) {
                return obj;
            }
            if (obj instanceof String) {
                try {
                    return ((Inet6Address) Inet6Address.getByName((String) obj));
                } catch (UnknownHostException | ClassCastException e) {
                    throw new ByteHouseSQLException(-1, obj + " is not a valid IPv6 address");
                }
            }
        }
        if (type instanceof DataTypeNothing) {
            return null;
        }
        if (type instanceof DataTypeNullable) {
            // handled null at first, so obj also not null here
            return convertToCkDataType(((DataTypeNullable) type).getNestedDataType(), obj);
        }
        if (type instanceof DataTypeArray) {
            if (!(obj instanceof ByteHouseArray)) {
                throw new ByteHouseSQLException(-1, "require ByteHouseArray for column: " + type.name() + ", but found " + obj.getClass());
            }
            return ((ByteHouseArray) obj).mapElements(unchecked(this::convertToCkDataType));
        }
        if (type instanceof DataTypeBitMap64) {
            if (!(obj instanceof ByteHouseArray)) {
                throw new ByteHouseSQLException(-1, "require ByteHouseArray for column: " + type.name() + ", but found " + obj.getClass());
            }
            return ((ByteHouseArray) obj).mapElements(unchecked(this::convertToCkDataType));
        }
        if (type instanceof DataTypeTuple) {
            if (!(obj instanceof ByteHouseStruct)) {
                throw new ByteHouseSQLException(-1, "require ByteHouseStruct for column: " + type.name() + ", but found " + obj.getClass());
            }
            return ((ByteHouseStruct) obj).mapAttributes(((DataTypeTuple) type).getNestedTypes(), unchecked(this::convertToCkDataType));
        }
        // TODO: 8/7/21 Throw an error here instead of letting it go downstream (do this in the new type cast system)
        LOG.debug("unhandled type: {}[{}]", type.name(), obj.getClass());
        return obj;
    }
}
