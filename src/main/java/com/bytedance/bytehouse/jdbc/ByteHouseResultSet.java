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

import com.bytedance.bytehouse.data.Block;
import com.bytedance.bytehouse.data.IColumn;
import com.bytedance.bytehouse.jdbc.wrapper.SQLResultSet;
import com.bytedance.bytehouse.log.Logger;
import com.bytedance.bytehouse.log.LoggerFactory;
import com.bytedance.bytehouse.exception.ByteHouseSQLException;
import com.bytedance.bytehouse.jdbc.statement.ByteHouseStatement;
import com.bytedance.bytehouse.misc.CheckedIterator;
import com.bytedance.bytehouse.misc.DateTimeUtil;
import com.bytedance.bytehouse.misc.Validate;
import com.bytedance.bytehouse.protocol.DataResponse;
import com.bytedance.bytehouse.settings.ByteHouseConfig;

import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.*;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.util.Calendar;

public class ByteHouseResultSet implements SQLResultSet {

    private static final Logger LOG = LoggerFactory.getLogger(ByteHouseResultSet.class);

    private int currentRowNum = -1;
    private Block currentBlock = new Block();

    private int lastFetchRowIdx = -1;
    private int lastFetchColumnIdx = -1;
    private Block lastFetchBlock = null;

    private final ByteHouseStatement statement;
    private final ByteHouseConfig cfg;
    private final String db;
    private final String table;
    private final Block header;
    private final CheckedIterator<DataResponse, SQLException> dataResponses;

    private boolean isFirst = false;
    private boolean isAfterLast = false;
    private boolean isClosed = false;

    public ByteHouseResultSet(ByteHouseStatement statement,
                              ByteHouseConfig cfg,
                              String db,
                              String table,
                              Block header,
                              CheckedIterator<DataResponse, SQLException> dataResponses) {
        this.statement = statement;
        this.cfg = cfg;
        this.db = db;
        this.table = table;
        this.header = header;
        this.dataResponses = dataResponses;
    }

    @Override
    public boolean getBoolean(String name) throws SQLException {
        return this.getBoolean(this.findColumn(name));
    }

    @Override
    public byte getByte(String name) throws SQLException {
        return this.getByte(this.findColumn(name));
    }

    @Override
    public short getShort(String name) throws SQLException {
        return this.getShort(this.findColumn(name));
    }

    @Override
    public int getInt(String name) throws SQLException {
        return this.getInt(this.findColumn(name));
    }

    @Override
    public long getLong(String name) throws SQLException {
        return this.getLong(this.findColumn(name));
    }

    @Override
    public float getFloat(String name) throws SQLException {
        return this.getFloat(this.findColumn(name));
    }

    @Override
    public double getDouble(String name) throws SQLException {
        return this.getDouble(this.findColumn(name));
    }

    @Override
    public Timestamp getTimestamp(String name) throws SQLException {
        return this.getTimestamp(this.findColumn(name));
    }

    @Override
    public Timestamp getTimestamp(String name, Calendar cal) throws SQLException {
        return this.getTimestamp(this.findColumn(name), cal);
    }

    @Override
    public Date getDate(String name) throws SQLException {
        return this.getDate(this.findColumn(name));
    }

    @Override
    public BigDecimal getBigDecimal(String name) throws SQLException {
        return this.getBigDecimal(this.findColumn(name));
    }

    @Override
    public String getString(String name) throws SQLException {
        return this.getString(this.findColumn(name));
    }

    @Override
    public byte[] getBytes(String name) throws SQLException {
        return this.getBytes(this.findColumn(name));
    }

    @Override
    public URL getURL(String name) throws SQLException {
        return this.getURL(this.findColumn(name));
    }

    @Override
    public Array getArray(String name) throws SQLException {
        return this.getArray(this.findColumn(name));
    }

    @Override
    public Object getObject(String name) throws SQLException {
        return this.getObject(this.findColumn(name));
    }

    @Override
    public boolean getBoolean(int position) throws SQLException {
        Object data = getInternalObject(position);
        if (data == null) {
            return false;
        }
        Number ndata = (Number) data;
        return (ndata.shortValue() != 0);
    }

    @Override
    public byte getByte(int position) throws SQLException {
        Object data = getInternalObject(position);
        if (data == null) {
            return 0;
        }
        return ((Number) data).byteValue();
    }

    @Override
    public short getShort(int position) throws SQLException {
        Object data = getInternalObject(position);
        if (data == null) {
            return 0;
        }
        return ((Number) data).shortValue();
    }

    @Override
    public int getInt(int position) throws SQLException {
        Object data = getInternalObject(position);
        if (data == null) {
            return 0;
        }
        return ((Number) data).intValue();
    }

    @Override
    public long getLong(int position) throws SQLException {
        Object data = getInternalObject(position);
        if (data == null) {
            return 0;
        }
        return ((Number) data).longValue();
    }

    @Override
    public float getFloat(int position) throws SQLException {
        Object data = getInternalObject(position);
        if (data == null) {
            return 0;
        }
        return ((Number) data).floatValue();
    }

    @Override
    public double getDouble(int position) throws SQLException {
        Object data = getInternalObject(position);
        if (data == null) {
            return 0;
        }
        return ((Number) data).doubleValue();
    }

    @Override
    public Timestamp getTimestamp(int position) throws SQLException {
        Object data = getInternalObject(position);
        if (data == null) {
            return null;
        }
        ZonedDateTime zts = (ZonedDateTime) data;
        return DateTimeUtil.toTimestamp(zts, null);
    }

    @Override
    public Timestamp getTimestamp(int position, Calendar cal) throws SQLException {
        Object data = getInternalObject(position);
        if (data == null) {
            return null;
        }
        ZonedDateTime zts = (ZonedDateTime) data;
        return DateTimeUtil.toTimestamp(zts, cal.getTimeZone().toZoneId());
    }

    @Override
    public Date getDate(int position) throws SQLException {
        LocalDate date = (LocalDate) getInternalObject(position);
        if (date == null)
            return null;
        return Date.valueOf(date);
    }

    @Override
    public BigDecimal getBigDecimal(int position) throws SQLException {
        Object data = getInternalObject(position);
        if (data == null) {
            return null;
        }
        if (data instanceof BigDecimal) {
            return ((BigDecimal) data);
        }
        return new BigDecimal(data.toString());
    }

    @Override
    public String getString(int position) throws SQLException {
        Object data = getInternalObject(position);
        if (data == null) {
            return null;
        }
        // TODO format by IDataType
        return data.toString();
    }

    @Override
    public byte[] getBytes(int position) throws SQLException {
        Object data = getInternalObject(position);
        if (data == null) {
            return null;
        }
        if (data instanceof String) {
            return ((String) data).getBytes(cfg.charset());
        }
        throw new ByteHouseSQLException(-1, "Currently not support getBytes from class: " + data.getClass());
    }

    @Override
    public URL getURL(int position) throws SQLException {
        String data = this.getString(position);
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
    public Array getArray(int position) throws SQLException {
        Object data = getInternalObject(position);
        return (Array) data;
    }

    @Override
    public Object getObject(int position) throws SQLException {
        Object obj = getInternalObject(position);
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

    private Object getInternalObject(int position) throws SQLException {
        LOG.trace("get object at row: {}, column position: {} from block with column count: {}, row count: {}",
                currentRowNum, position, currentBlock.columnCnt(), currentBlock.rowCnt());
        Validate.isTrue(currentRowNum >= 0 && currentRowNum < currentBlock.rowCnt(),
                "No row information was obtained. You must call ResultSet.next() before that.");
        IColumn column = (lastFetchBlock = currentBlock).getColumn((lastFetchColumnIdx = position - 1));
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

    @Override
    public int getFetchDirection() throws SQLException {
        return ResultSet.FETCH_FORWARD;
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
        Validate.isTrue(lastFetchBlock != null, "Please call Result.next()");
        Validate.isTrue(lastFetchColumnIdx >= 0, "Please call Result.getXXX()");
        Validate.isTrue(lastFetchRowIdx >= 0 && lastFetchRowIdx < lastFetchBlock.rowCnt(), "Please call Result.next()");
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
    public int findColumn(String name) throws SQLException {
        LOG.trace("find column: {}", name);
        return header.getPositionByName(name);
    }

    @Override
    public boolean next() throws SQLException {
        boolean isBeforeFirst = isBeforeFirst();
        LOG.trace("check status[before]: is_before_first: {}, is_first: {}, is_after_last: {}", isBeforeFirst, isFirst, isAfterLast);

        boolean hasNext = ++currentRowNum < currentBlock.rowCnt() || (currentRowNum = 0) < (currentBlock = fetchBlock()).rowCnt();

        isFirst = isBeforeFirst && hasNext;
        isAfterLast = !hasNext;
        LOG.trace("check status[after]: has_next: {}, is_before_first: {}, is_first: {}, is_after_last: {}", hasNext, isBeforeFirst(), isFirst, isAfterLast);

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
        while (dataResponses.hasNext()) {
            dataResponses.next();
        }
        // reset variables
        currentBlock = new Block();
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
            DataResponse next = dataResponses.next();
            if (next.block().rowCnt() > 0) {
                return next.block();
            }
        }
        LOG.debug("no more DataResponse, return empty Block");
        return new Block();
    }
}