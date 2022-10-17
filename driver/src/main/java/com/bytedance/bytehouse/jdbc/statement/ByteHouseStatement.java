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

import com.bytedance.bytehouse.data.Block;
import com.bytedance.bytehouse.jdbc.ByteHouseConnection;
import com.bytedance.bytehouse.jdbc.ByteHouseResultSet;
import com.bytedance.bytehouse.jdbc.wrapper.SQLWrapper;
import com.bytedance.bytehouse.log.Logger;
import com.bytedance.bytehouse.log.LoggerFactoryUtils;
import com.bytedance.bytehouse.log.Logging;
import com.bytedance.bytehouse.misc.ExceptionUtil;
import com.bytedance.bytehouse.misc.SQLParserUtils;
import com.bytedance.bytehouse.misc.SqlParserCaseExpressionUtils;
import com.bytedance.bytehouse.misc.SqlParserDateFormatUtils;
import com.bytedance.bytehouse.misc.SqlParserOrExpressionUtils;
import com.bytedance.bytehouse.misc.ValidateUtils;
import com.bytedance.bytehouse.settings.ByteHouseConfig;
import com.bytedance.bytehouse.settings.SettingKey;
import com.bytedance.bytehouse.stream.QueryResult;
import com.bytedance.bytehouse.stream.ValuesNativeInputFormat;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.time.Duration;
import java.util.regex.Pattern;

/**
 * {@link Statement} implementation for Bytehouse.
 */
public class ByteHouseStatement implements Statement, SQLWrapper, Logging {

    static final Pattern TXN_LABEL_REGEX = Pattern
            .compile("insertion_label\\s?=\\s?'[a-zA-Z0-9\\-]+'");

    private static final Logger LOG = LoggerFactoryUtils.getLogger(ByteHouseStatement.class);

    private static final Pattern VALUES_REGEX = Pattern
            .compile("[V|v][A|a][L|l][U|u][E|e][S|s]\\s*\\(");

    private static final Pattern SELECT_DB_TABLE = Pattern.compile("(?i)FROM\\s+(\\S+\\.)?(\\S+)");

    protected final ByteHouseConnection creator;

    protected final String defaultDb;

    protected Block block;

    protected ByteHouseConfig cfg;

    // =========  START: temporary variables per execution ===========
    protected ResultSet lastResultSet;

    private long maxRows;

    private int updateCount = -1;
    // =========  END: temporary variables per execution ===========

    private boolean isClosed = false;

    public ByteHouseStatement(
            final ByteHouseConnection connection
    ) {
        this.creator = connection;
        this.cfg = connection.cfg();
        this.defaultDb = cfg.database();
    }

    /**
     * Doesn't support returning multiple result set as documented in Statement interface.
     * Only tracks the one and only ResultSet / updateCount returned by query.
     */
    @Override
    public boolean execute(final String query) throws SQLException {
        // Resolve case expression
        if (creator.cfg().isTableau()) {
            final String modifiedQuery = SqlParserCaseExpressionUtils.resolve(query);
            final String modifiedQuery2 = SqlParserDateFormatUtils.resolve(modifiedQuery);
            final String modifiedQuery3 = SqlParserOrExpressionUtils.resolve(modifiedQuery2);
            return executeQuery(modifiedQuery3) != null;
        }
        return executeQuery(query) != null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int executeUpdate(final String query) throws SQLException {
        // closes current ResultSet if it exists before running another query.
        if (lastResultSet != null) {
            lastResultSet.close();
        }

        return ExceptionUtil.rethrowSQLException(() -> {

            if (SQLParserUtils.isInsertQuery(query)) {
                // insert statement we return row count.
                lastResultSet = null; // NOPMD assigning null smells

                final SQLParserUtils.InsertQueryParts parts = SQLParserUtils.splitInsertQuery(query);
                final String insertQuery = parts.queryPart;
                block = creator.getSampleBlock(insertQuery);
                block.initWriteBuffer();
                new ValuesNativeInputFormat(0, parts.valuePart).fill(block);
                updateCount = creator.sendInsertRequest(block);
                return updateCount;
            } else {
                final SQLParserUtils.DbTable dbTable = SQLParserUtils.extractDBAndTableName(query);
                // other statement we return 0.
                updateCount = -1;
                final QueryResult result = creator.sendQueryRequest(query, cfg);
                lastResultSet = new ByteHouseResultSet(
                        this,
                        cfg,
                        dbTable.getDbOrDefault(this.defaultDb),
                        dbTable.getTable(),
                        result.header(),
                        result.data()
                );
                return 0;
            }
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResultSet executeQuery(final String query) throws SQLException {
        executeUpdate(query);
        return getResultSet();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getUpdateCount() throws SQLException {
        return updateCount;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResultSet getResultSet() {
        return lastResultSet;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean getMoreResults() throws SQLException {
        updateCount = -1;
        if (lastResultSet != null) {
            lastResultSet.close();
            lastResultSet = null;
        }
        return false;
    }

    /**
     * Releases all resources. If current ResultSet exists, it is also closed.
     */
    @Override
    public void close() throws SQLException {
        LOG.debug("close Statement");
        if (lastResultSet != null) {
            lastResultSet.close();
        }
        this.isClosed = true;
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        return 0;
    }

    @Override
    public void setMaxFieldSize(final int max) throws SQLException {

    }

    @Override
    public boolean isClosed() throws SQLException {
        return this.isClosed;
    }

    @Override
    public void cancel() throws SQLException {
        LOG.debug("cancel Statement");
        // TODO send cancel request and clear responses
        this.close();
    }

    @Override
    public int getMaxRows() throws SQLException {
        return (int) maxRows;
    }

    @Override
    public void setMaxRows(final int max) throws SQLException {
        ValidateUtils.isTrue(max >= 0, "Illegal maxRows value: " + max);
        maxRows = max;
        cfg.settings().put(SettingKey.max_result_rows, maxRows);
    }

    @Override
    public void setEscapeProcessing(final boolean enable) throws SQLException {

    }

    // JDBC returns timeout in seconds
    @Override
    public int getQueryTimeout() {
        return (int) cfg.queryTimeout().getSeconds();
    }

    @Override
    public void setQueryTimeout(final int seconds) {
        this.cfg = cfg.withQueryTimeout(Duration.ofSeconds(seconds));
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
     * currently not supported in ResultSet.
     */
    @Override
    public void setFetchDirection(final int direction) throws SQLException {
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
    public boolean isPoolable() throws SQLException {
        return false;
    }

    @Override
    public void closeOnCompletion() throws SQLException {

    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        return false;
    }

    /**
     * Statement pooling is not supported, so it cannot be set to true.
     */
    @Override
    public void setPoolable(final boolean poolable) throws SQLException {
        if (poolable) {
            throw new SQLException("statement not poolable.");
        }
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public int getResultSetType() throws SQLException {
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public void addBatch(final String sql) throws SQLException {

    }

    @Override
    public void clearBatch() throws SQLException {

    }

    @Override
    public int[] executeBatch() throws SQLException {
        return new int[0];
    }

    @Override
    public Connection getConnection() {
        return creator;
    }

    @Override
    public boolean getMoreResults(final int current) throws SQLException {
        return false;
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        return null;
    }

    @Override
    public int executeUpdate(final String sql, final int autoGeneratedKeys) throws SQLException {
        return 0;
    }

    @Override
    public int executeUpdate(final String sql, final int[] columnIndexes) throws SQLException {
        return 0;
    }

    @Override
    public int executeUpdate(final String sql, final String[] columnNames) throws SQLException {
        return 0;
    }

    @Override
    public boolean execute(final String sql, final int autoGeneratedKeys) throws SQLException {
        return false;
    }

    @Override
    public boolean execute(final String sql, final int[] columnIndexes) throws SQLException {
        return false;
    }

    @Override
    public boolean execute(final String sql, final String[] columnNames) throws SQLException {
        return false;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
    }

    @Override
    public void setCursorName(final String name) throws SQLException {

    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public Logger logger() {
        return ByteHouseStatement.LOG;
    }
}
