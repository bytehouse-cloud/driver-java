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
import com.bytedance.bytehouse.exception.ByteHouseClientException;
import com.bytedance.bytehouse.jdbc.ByteHouseConnection;
import com.bytedance.bytehouse.misc.DateTimeUtil;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

public class ByteHousePreparedQueryStatement extends AbstractPreparedStatement {

    public ByteHousePreparedQueryStatement(
            final ByteHouseConnection conn,
            final ServerContext serverContext,
            final String query
    ) {
        this(conn, serverContext, splitQueryByQuestionMark(query));
    }

    private ByteHousePreparedQueryStatement(
            final ByteHouseConnection conn,
            final ServerContext serverContext,
            final String[] parts
    ) {
        super(conn, serverContext, parts);
    }

    private static String[] splitQueryByQuestionMark(final String query) {
        int lastPos = 0;
        final List<String> queryParts = new ArrayList<>();
        boolean inQuotes = false, inBackQuotes = false;
        for (int i = 0; i < query.length(); i++) {
            final char ch = query.charAt(i);
            if (ch == '`') {
                inBackQuotes = !inBackQuotes;
            } else if (ch == '\'') {
                inQuotes = !inQuotes;
            } else if (!inBackQuotes && !inQuotes) {
                if (ch == '?') {
                    queryParts.add(query.substring(lastPos, i));
                    lastPos = i + 1;
                }
            }
        }
        queryParts.add(query.substring(lastPos));
        return queryParts.toArray(new String[0]);
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        if (getResultSet() == null) {
            return this.executeQuery().getMetaData();
        }
        else {
            return getResultSet().getMetaData();
        }
    }

    @Override
    public void setObject(final int idx, final Object x) throws SQLException {
        parameters[idx - 1] = convertObjectIfNecessary(x);
    }

    @Override
    public boolean execute() throws SQLException {
        return execute(assembleQueryPartsAndParameters());
    }

    @Override
    public int executeUpdate() throws SQLException {
        return executeUpdate(assembleQueryPartsAndParameters());
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        return executeQuery(assembleQueryPartsAndParameters());
    }

    @Override
    public String toString() {
        final StringBuilder queryBuilder = new StringBuilder();
        queryBuilder.append(super.toString());
        try {
            queryBuilder.append(": ");
            queryBuilder.append(assembleQueryPartsAndParameters());
        } catch (Exception e) {
            throw new ByteHouseClientException(e);
        }
        return queryBuilder.toString();
    }

    private Object convertObjectIfNecessary(final Object obj) {
        Object result = obj;
        if (obj instanceof Date) {
            result = ((Date) obj).toLocalDate();
        }
        if (obj instanceof Timestamp) {
            result = DateTimeUtil.toZonedDateTime((Timestamp) obj, tz);
        }
        return result;
    }
}
