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

import com.bytedance.bytehouse.client.ServerContext;
import com.bytedance.bytehouse.jdbc.ByteHouseConnection;
import com.bytedance.bytehouse.jdbc.wrapper.SQLPreparedStatement;
import com.bytedance.bytehouse.misc.BytesCharSeq;
import com.bytedance.bytehouse.misc.DateTimeUtil;
import com.bytedance.bytehouse.misc.ValidateUtils;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Date;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Struct;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Locale;
import java.util.regex.Matcher;

/**
 * contains some metadata shared by all PreparedStatement.
 */
public abstract class AbstractPreparedStatement
        extends ByteHouseStatement
        implements SQLPreparedStatement {

    protected final ZoneId tz;

    private final String[] queryParts;

    private final DateTimeFormatter dateFmt;

    private final DateTimeFormatter timestampFmt;

    protected Object[] parameters;

    /**
     * Constructor.
     */
    public AbstractPreparedStatement(
            final ByteHouseConnection connection,
            final ServerContext serverContext,
            final String[] queryParts
    ) {
        super(connection);
        this.queryParts = queryParts;
        if (queryParts != null && queryParts.length > 0)
            this.parameters = new Object[queryParts.length];

        this.tz = DateTimeUtil.chooseTimeZone(serverContext);
        this.dateFmt = DateTimeFormatter
                .ofPattern("yyyy-MM-dd", Locale.ROOT).withZone(tz);
        this.timestampFmt = DateTimeFormatter
                .ofPattern("yyyy-MM-dd HH:mm:ss", Locale.ROOT).withZone(tz);
    }

    @Override
    public void setBoolean(final int index, final boolean x) throws SQLException {
        setObject(index, x ? (byte) 1 : (byte) 0);
    }

    @Override
    public void setByte(final int index, final byte x) throws SQLException {
        setObject(index, x);
    }

    @Override
    public void setShort(final int index, final short x) throws SQLException {
        setObject(index, x);
    }

    @Override
    public void setInt(final int index, final int x) throws SQLException {
        setObject(index, x);
    }

    @Override
    public void setLong(final int index, final long x) throws SQLException {
        setObject(index, x);
    }

    @Override
    public void setFloat(final int index, final float x) throws SQLException {
        setObject(index, x);
    }

    @Override
    public void setDouble(final int index, final double x) throws SQLException {
        setObject(index, x);
    }

    @Override
    public void setNull(final int index, final int type) throws SQLException {
        setObject(index, null);
    }

    @Override
    public void setTimestamp(final int index, final Timestamp x) throws SQLException {
        setObject(index, DateTimeUtil.toZonedDateTime(x, tz));
    }

    @Override
    public void setTimestamp(
            final int index,
            final Timestamp x,
            final Calendar cal
    ) throws SQLException {
        setObject(index, DateTimeUtil.toZonedDateTime(x, cal.getTimeZone().toZoneId()));
    }

    @Override
    public void setDate(final int index, final Date x) throws SQLException {
        setObject(index, x.toLocalDate());
    }

    @Override
    public void setDate(final int index, final Date x, final Calendar cal) throws SQLException {
        // just ignore cal, date has no concepts of timezone
        setObject(index, x.toLocalDate());
    }

    @Override
    public void setBigDecimal(final int index, final BigDecimal x) throws SQLException {
        setObject(index, x);
    }

    @Override
    public void setString(final int index, final String x) throws SQLException {
        setObject(index, x);
    }

    @Override
    public void setBytes(final int index, final byte[] x) throws SQLException {
        setObject(index, new BytesCharSeq(x));
    }

    @Override
    public void setURL(final int index, final URL x) throws SQLException {
        setObject(index, x);
    }

    @Override
    public void setArray(final int index, final Array x) throws SQLException {
        setObject(index, x);
    }

    @Override
    public void setObject(final int index, Object x, final int targetSqlType) throws SQLException {
        setObject(index, x);
    }

    @Override
    public void setObject(
            final int index, Object x,
            final int targetSqlType,
            final int scaleOrLength
    ) throws SQLException {
        setObject(index, x);
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return getResultSet().getMetaData();
    }

    @Override
    public void clearParameters() throws SQLException {
        Arrays.fill(parameters, null);
    }

    protected String assembleQueryPartsAndParameters() throws SQLException {
        // TODO: move to DataType
        StringBuilder queryBuilder = new StringBuilder();
        for (int i = 0; i < queryParts.length; i++) {
            if (i - 1 >= 0 && i - 1 < parameters.length) {
                ValidateUtils.isTrue(assembleParameter(parameters[i - 1], queryBuilder),
                        "UNKNOWN DataType :" + (parameters[i - 1] == null ? null : parameters[i - 1].getClass()));
            }
            queryBuilder.append(queryParts[i]);
        }
        return queryBuilder.toString();
    }

    private boolean assembleParameter(
            final Object parameter,
            final StringBuilder queryBuilder
    ) throws SQLException {
        return assembleSimpleParameter(queryBuilder, parameter)
                || assembleComplexQuotedParameter(queryBuilder, parameter);
    }

    private boolean assembleSimpleParameter(
            final StringBuilder queryBuilder,
            final Object parameter
    ) {
        if (parameter instanceof String)
            return assembleQuotedParameter(queryBuilder, String.valueOf(parameter));
        if (parameter == null)
            return assembleWithoutQuotedParameter(queryBuilder, "Null");
        if (parameter instanceof Number)
            return assembleWithoutQuotedParameter(queryBuilder, parameter);
        if (parameter instanceof LocalDate)
            return assembleQuotedParameter(queryBuilder, dateFmt.format((LocalDate) parameter));
        if (parameter instanceof ZonedDateTime)
            return assembleQuotedParameter(queryBuilder, timestampFmt.format((ZonedDateTime) parameter));
        return false;
    }

    private boolean assembleQuotedParameter(
            final StringBuilder queryBuilder,
            final String parameter
    ) {
        queryBuilder
                .append('\'')
                .append(parameter.replaceAll("'", Matcher.quoteReplacement("\\'")))
                .append('\'');
        return true;
    }

    private boolean assembleWithoutQuotedParameter(
            final StringBuilder queryBuilder,
            final Object parameter
    ) {
        queryBuilder.append(parameter);
        return true;
    }

    private boolean assembleComplexQuotedParameter(
            final StringBuilder queryBuilder,
            final Object parameter
    ) throws SQLException {
        if (parameter instanceof Array) {
            queryBuilder.append('[');
            final Object[] arrayData = (Object[]) ((Array) parameter).getArray();
            for (int arrayIndex = 0; arrayIndex < arrayData.length; arrayIndex++) {
                assembleParameter(arrayData[arrayIndex], queryBuilder);
                queryBuilder.append(arrayIndex == arrayData.length - 1 ? "]" : ",");
            }
            return true;
        } else if (parameter instanceof Struct) {
            queryBuilder.append('(');
            final Object[] structData = ((Struct) parameter).getAttributes();
            for (int structIndex = 0; structIndex < structData.length; structIndex++) {
                assembleParameter(structData[structIndex], queryBuilder);
                queryBuilder.append(structIndex == structData.length - 1 ? ")" : ",");
            }
            return true;
        }
        return false;
    }
}
