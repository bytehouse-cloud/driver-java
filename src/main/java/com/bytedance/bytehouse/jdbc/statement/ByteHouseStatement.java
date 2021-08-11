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
import com.bytedance.bytehouse.jdbc.ByteHouseConnection;
import com.bytedance.bytehouse.jdbc.ByteHouseResultSet;
import com.bytedance.bytehouse.jdbc.wrapper.SQLStatement;
import com.bytedance.bytehouse.log.Logger;
import com.bytedance.bytehouse.log.LoggerFactory;
import com.bytedance.bytehouse.misc.ExceptionUtil;
import com.bytedance.bytehouse.misc.Validate;
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
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * {@link Statement} implementation for Bytehouse.
 */
public class ByteHouseStatement implements SQLStatement {

    private static final Logger LOG = LoggerFactory.getLogger(ByteHouseStatement.class);

    private static final Pattern VALUES_REGEX = Pattern
            .compile("[V|v][A|a][L|l][U|u][E|e][S|s]\\s*\\(");

    private static final Pattern SELECT_DB_TABLE = Pattern.compile("(?i)FROM\\s+(\\S+\\.)?(\\S+)");

    protected final ByteHouseConnection creator;

    protected final NativeContext nativeContext;

    protected Block block;

    private ResultSet lastResultSet;

    private ByteHouseConfig cfg;

    private long maxRows;

    // =========  START: temporary variables per execution ===========
    private String db;

    private String table = "unknown";

    private int updateCount = -1;
    // =========  END: temporary variables per execution ===========

    private boolean isClosed = false;

    public ByteHouseStatement(
            final ByteHouseConnection connection,
            final NativeContext nativeContext
    ) {
        this.creator = connection;
        this.nativeContext = nativeContext;
        this.cfg = connection.cfg();
        this.db = cfg.database();
    }

    /**
     * Doesn't support returning multiple result set as documented in Statement interface.
     * Only tracks the one and only ResultSet / updateCount returned by query.
     */
    @Override
    public boolean execute(final String query) throws SQLException {
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
            extractDBAndTableName(query);
            final Matcher matcher = VALUES_REGEX.matcher(query);

            if (matcher.find() && query.trim().toUpperCase(Locale.ROOT).startsWith("INSERT")) {
                // insert statement we return row count.
                lastResultSet = null;
                final String insertQuery = query.substring(0, matcher.end() - 1);
                block = creator.getSampleBlock(insertQuery);
                block.initWriteBuffer();
                new ValuesNativeInputFormat(matcher.end() - 1, query).fill(block);
                updateCount = creator.sendInsertRequest(block);
                return updateCount;
            } else {
                // other statement we return 0.
                updateCount = -1;
                final QueryResult result = creator.sendQueryRequest(query, cfg);
                lastResultSet = new ByteHouseResultSet(
                        this,
                        cfg,
                        db,
                        table,
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
        Validate.isTrue(max >= 0, "Illegal maxRows value: " + max);
        maxRows = max;
        cfg.settings().put(SettingKey.max_result_rows, maxRows);
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
    public Connection getConnection() {
        return creator;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public Logger logger() {
        return ByteHouseStatement.LOG;
    }

    private void extractDBAndTableName(final String sql) {
        final String upperSQL = sql.trim().toUpperCase(Locale.ROOT);
        if (upperSQL.startsWith("SELECT")) {
            final Matcher m = SELECT_DB_TABLE.matcher(sql);
            if (m.find()) {
                if (m.groupCount() == 2) {
                    if (m.group(1) != null) {
                        db = m.group(1);
                    }
                    table = m.group(2);
                }
            }
        } else if (upperSQL.startsWith("DESC")) {
            db = "system";
            table = "columns";
        } else if (upperSQL.startsWith("SHOW")) {
            db = "system";
            table = upperSQL.contains("TABLES") ? "tables" : "databases";
        }
    }
}
