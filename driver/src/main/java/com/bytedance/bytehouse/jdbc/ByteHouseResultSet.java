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

import com.bytedance.bytehouse.data.Block;
import com.bytedance.bytehouse.data.IColumn;
import com.bytedance.bytehouse.exception.ByteHouseSQLException;
import com.bytedance.bytehouse.jdbc.statement.ByteHouseStatement;
import com.bytedance.bytehouse.jdbc.wrapper.SQLResultSet;
import com.bytedance.bytehouse.log.Logger;
import com.bytedance.bytehouse.log.LoggerFactoryUtils;
import com.bytedance.bytehouse.misc.CheckedIterator;
import com.bytedance.bytehouse.misc.DateTimeUtil;
import com.bytedance.bytehouse.misc.ValidateUtils;
import com.bytedance.bytehouse.protocol.DataResponse;
import com.bytedance.bytehouse.settings.ByteHouseConfig;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Array;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.util.Calendar;

/**
 * Bytehouse implementation of {@link ResultSet}.
 */
public class ByteHouseResultSet implements SQLResultSet {

    private static final Logger LOG = LoggerFactoryUtils.getLogger(ByteHouseResultSet.class);

    private final ByteHouseStatement statement;

    private final ByteHouseConfig cfg;

    private final String db;

    private final String table;

    private final Block header;

    private final CheckedIterator<DataResponse, SQLException> dataResponses;

    private int currentRowNum = -1;

    private Block currentBlock = Block.empty();

    private int lastFetchRowIdx = -1;

    private int lastFetchColumnIdx = -1;

    private Block lastFetchBlock;

    private boolean isFirst;

    private boolean isAfterLast;

    private boolean isClosed;

    /**
     * Constructor.
     */
    public ByteHouseResultSet(
            final ByteHouseStatement statement,
            final ByteHouseConfig cfg,
            final String db,
            final String table,
            final Block header,
            final CheckedIterator<DataResponse, SQLException> dataResponses
    ) {
        this.statement = statement;
        this.cfg = cfg;
        this.db = db;
        this.table = table;
        this.header = header;
        this.dataResponses = dataResponses;
    }

    @Override
    public boolean getBoolean(final String name) throws SQLException {
        return this.getBoolean(this.findColumn(name));
    }

    @Override
    public byte getByte(final String name) throws SQLException {
        return this.getByte(this.findColumn(name));
    }

    @Override
    public short getShort(final String name) throws SQLException {
        return this.getShort(this.findColumn(name));
    }

    @Override
    public int getInt(final String name) throws SQLException {
        return this.getInt(this.findColumn(name));
    }

    @Override
    public long getLong(final String name) throws SQLException {
        return this.getLong(this.findColumn(name));
    }

    @Override
    public float getFloat(final String name) throws SQLException {
        return this.getFloat(this.findColumn(name));
    }

    @Override
    public double getDouble(final String name) throws SQLException {
        return this.getDouble(this.findColumn(name));
    }

    @Override
    public Timestamp getTimestamp(final String name) throws SQLException {
        return this.getTimestamp(this.findColumn(name));
    }

    @Override
    public Timestamp getTimestamp(final String name, final Calendar cal) throws SQLException {
        return this.getTimestamp(this.findColumn(name), cal);
    }

    @Override
    public Date getDate(final String name) throws SQLException {
        return this.getDate(this.findColumn(name));
    }

    @Override
    public Time getTime(final String columnLabel) throws SQLException {
        return getTime(this.findColumn(columnLabel));
    }

    @Override
    public BigDecimal getBigDecimal(final String name) throws SQLException {
        return this.getBigDecimal(this.findColumn(name));
    }

    @Override
    public String getString(final String name) throws SQLException {
        return this.getString(this.findColumn(name));
    }

    @Override
    public byte[] getBytes(final String name) throws SQLException {
        return this.getBytes(this.findColumn(name));
    }

    @Override
    public URL getURL(final String name) throws SQLException {
        return this.getURL(this.findColumn(name));
    }

    @Override
    public Array getArray(final String name) throws SQLException {
        return this.getArray(this.findColumn(name));
    }

    @Override
    public Date getDate(final int columnIndex, final Calendar cal) throws SQLException {
        final LocalDate date = (LocalDate) getInternalObject(columnIndex);
        if (date == null) {
            return null;
        }
        cal.set(date.getYear(), date.getMonthValue() - 1, date.getDayOfMonth(), 0, 0, 0);
        return new Date(cal.getTimeInMillis());
    }

    @Override
    public Date getDate(final String columnLabel, final Calendar cal) throws SQLException {
        return getDate(columnLabel);
    }

    @Override
    public Object getObject(final String name) throws SQLException {
        return this.getObject(this.findColumn(name));
    }

    @Override
    public boolean getBoolean(final int position) throws SQLException {
        final Object data = getInternalObject(position);
        if (data == null) {
            return false;
        }
        final Number ndata = (Number) data;
        return (ndata.shortValue() != 0);
    }

    @Override
    public byte getByte(final int position) throws SQLException {
        final Object data = getInternalObject(position);
        if (data == null) {
            return 0;
        }
        return ((Number) data).byteValue();
    }

    @Override
    public short getShort(final int position) throws SQLException {
        final Object data = getInternalObject(position);
        if (data == null) {
            return 0;
        }
        return ((Number) data).shortValue();
    }

    @Override
    public int getInt(final int position) throws SQLException {
        final Object data = getInternalObject(position);
        if (data == null) {
            return 0;
        }
        return ((Number) data).intValue();
    }

    @Override
    public long getLong(final int position) throws SQLException {
        final Object data = getInternalObject(position);
        if (data == null) {
            return 0;
        }
        return ((Number) data).longValue();
    }

    @Override
    public float getFloat(final int position) throws SQLException {
        final Object data = getInternalObject(position);
        if (data == null) {
            return 0;
        }
        return ((Number) data).floatValue();
    }

    @Override
    public double getDouble(final int position) throws SQLException {
        final Object data = getInternalObject(position);
        if (data == null) {
            return 0;
        }
        return ((Number) data).doubleValue();
    }

    @Override
    public Timestamp getTimestamp(final int position) throws SQLException {
        final Object data = getInternalObject(position);
        if (data == null) {
            return null;
        }
        final ZonedDateTime zts = (ZonedDateTime) data;
        return DateTimeUtil.toTimestamp(zts, null);
    }

    @Override
    public Timestamp getTimestamp(final int position, final Calendar cal) throws SQLException {
        final Object data = getInternalObject(position);
        if (data == null) {
            return null;
        }
        final ZonedDateTime zts = (ZonedDateTime) data;
        cal.set(zts.getYear(), zts.getMonthValue() - 1, zts.getDayOfMonth(), zts.getHour(), zts.getMinute(),
                zts.getSecond());
        Timestamp timestamp = new Timestamp(cal.getTimeInMillis());
        timestamp.setNanos(zts.getNano());

        return timestamp;
    }

    @Override
    public Date getDate(final int position) throws SQLException {
        final LocalDate date = (LocalDate) getInternalObject(position);
        if (date == null)
            return null;
        return Date.valueOf(date);
    }

    @Override
    public Time getTime(final int columnIndex) throws SQLException {
        final ZonedDateTime dateTime = (ZonedDateTime) getInternalObject(columnIndex);
        return new Time(dateTime.getHour(), dateTime.getMinute(), dateTime.getSecond());
    }

    @Override
    public BigDecimal getBigDecimal(final int position) throws SQLException {
        final Object data = getInternalObject(position);
        if (data == null) {
            return null;
        }
        if (data instanceof BigDecimal) {
            return ((BigDecimal) data);
        }
        return new BigDecimal(data.toString());
    }

    @Override
    public String getString(final int position) throws SQLException {
        final Object data = getInternalObject(position);
        if (data == null) {
            return null;
        }
        // TODO format by IDataType
        return data.toString();
    }

    @Override
    public byte[] getBytes(final int position) throws SQLException {
        final Object data = getInternalObject(position);
        if (data == null) {
            return null;
        }
        if (data instanceof String) {
            return ((String) data).getBytes(cfg.charset());
        }
        throw new ByteHouseSQLException(-1, "Currently not support getBytes from class: "
                + data.getClass());
    }

    @Override
    public URL getURL(final int position) throws SQLException {
        final String data = this.getString(position);
        if (data == null) {
            return null;
        }
        try {
            return new URL(data);
        } catch (MalformedURLException ex) {
            throw new SQLException(ex.getMessage(), ex);
        }
    }

    @Override
    public Array getArray(final int position) throws SQLException {
        final Object data = getInternalObject(position);
        return (Array) data;
    }

    @Override
    public Object getObject(final int position) throws SQLException {
        final Object obj = getInternalObject(position);
        if (obj == null) {
            return null;
        }
        if (obj instanceof ZonedDateTime) {
            return DateTimeUtil.toTimestamp((ZonedDateTime) obj, null);
        }
        if (obj instanceof LocalDate) {
            return Date.valueOf(((LocalDate) obj));
        }
        // It's not necessary, because we always return a String, but keep it here for future refactor.
        // if (obj instanceof BytesCharSeq) {
        //    return ((BytesCharSeq) obj).bytes();
        // }
        return obj;
    }

    private Object getInternalObject(final int position) throws SQLException {
        if (LOG.isTraceEnabled()) {
            LOG.trace("get object at row: {}, column position: {} from block with "
                            + "column count: {}, row count: {}",
                    currentRowNum, position, currentBlock.columnCnt(), currentBlock.rowCnt());
        }
        ValidateUtils.isTrue(currentRowNum >= 0 && currentRowNum < currentBlock.rowCnt(),
                "No row information was obtained. You must call "
                        + "ResultSet.next() before that.");
        final IColumn column = (lastFetchBlock = currentBlock).getColumn(
                (lastFetchColumnIdx = position - 1)
        );
        return column.value((lastFetchRowIdx = currentRowNum));
    }

    @Override
    public boolean first() throws SQLException {
        throw new SQLException("TYPE_FORWARD_ONLY");
    }

    @Override
    public boolean last() throws SQLException {
        throw new SQLException("TYPE_FORWARD_ONLY");
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        return currentRowNum == -1;
    }

    @Override
    public boolean isFirst() throws SQLException {
        return isFirst;
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        return isAfterLast;
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return ResultSet.FETCH_FORWARD;
    }

    @Override
    public void setFetchSize(final int rows) throws SQLException {

    }

    /**
     * direction cannot be changed from FETCH_FORWARD, as other directions are
     * currently not supported.
     */
    @Override
    public void setFetchDirection(int direction) throws SQLException {
        if (direction != ResultSet.FETCH_FORWARD) {
            throw new SQLException("direction is not supported. FETCH_FORWARD only.");
        }
    }

    /**
     * Returns 0 to indicate that the driver will decide what the fetchSize should be.
     * User should set max_block_size in query settings to alter fetchSize instead.
     */
    @Override
    public int getFetchSize() throws SQLException {
        return 0;
    }

    @Override
    public int getType() throws SQLException {
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public int getConcurrency() throws SQLException {
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return new ByteHouseResultSetMetaData(header, db, table);
    }

    @Override
    public boolean wasNull() throws SQLException {
        ValidateUtils.isTrue(lastFetchBlock != null, "Please call Result.next()");
        ValidateUtils.isTrue(lastFetchColumnIdx >= 0, "Please call Result.getXXX()");
        ValidateUtils.isTrue(
                lastFetchRowIdx >= 0 && lastFetchRowIdx < lastFetchBlock.rowCnt(),
                "Please call Result.next()"
        );
        return lastFetchBlock.getColumn(lastFetchColumnIdx).value(lastFetchRowIdx) == null;
    }

    @Override
    public int getHoldability() throws SQLException {
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public Statement getStatement() throws SQLException {
        return statement;
    }

    @Override
    public int findColumn(final String name) throws SQLException {
        LOG.trace("find column: {}", name);
        return header.getPositionByName(name);
    }

    @Override
    public boolean next() throws SQLException {
        final boolean isBeforeFirst = isBeforeFirst();
        LOG.trace("check status[before]: is_before_first: {}, is_first: {}, is_after_last: {}",
                isBeforeFirst, isFirst, isAfterLast);

        final boolean hasNext = (++currentRowNum < currentBlock.rowCnt())
                || (currentRowNum = 0) < (currentBlock = fetchBlock()).rowCnt();

        isFirst = isBeforeFirst && hasNext;
        isAfterLast = !hasNext;
        if (LOG.isTraceEnabled()) {
            LOG.trace("check status[after]: has_next: {}, is_before_first: {}, is_first: {}, "
                    + "is_after_last: {}", hasNext, isBeforeFirst(), isFirst, isAfterLast);
        }

        return hasNext;
    }

    /**
     * Consumes remaining server responses and set the ResultSet as closed.
     *
     * ByteHouse sends the full query response to the socket, so this method has to consume all the data from the
     * socket to free the socket up upon closing for receiving other queries' responses.
     */
    @Override
    public void close() throws SQLException {
        // consume remaining responses
        if (dataResponses != null) {
            while (dataResponses.hasNext()) {
                dataResponses.next();
            }
        }
        // reset variables
        currentBlock = Block.empty();
        currentRowNum = 0;
        isFirst = false;
        isAfterLast = true;
        this.isClosed = true;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return this.isClosed;
    }

    @Override
    public Logger logger() {
        return ByteHouseResultSet.LOG;
    }

    private Block fetchBlock() throws SQLException {
        while (dataResponses.hasNext()) {
            LOG.trace("fetch next DataResponse");
            final DataResponse next = dataResponses.next();
            if (next.block().rowCnt() > 0) {
                return next.block();
            }
        }
        LOG.debug("no more DataResponse, return empty Block");
        return Block.empty();
    }
}
