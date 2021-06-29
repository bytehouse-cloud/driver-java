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
import com.bytedance.bytehouse.jdbc.ByteHouseConnection;
import com.bytedance.bytehouse.jdbc.ByteHouseResultSet;
import com.bytedance.bytehouse.jdbc.wrapper.SQLStatement;
import com.bytedance.bytehouse.log.Logger;
import com.bytedance.bytehouse.log.LoggerFactory;
import com.bytedance.bytehouse.client.NativeContext;
import com.bytedance.bytehouse.misc.ExceptionUtil;
import com.bytedance.bytehouse.misc.Validate;
import com.bytedance.bytehouse.stream.QueryResult;
import com.bytedance.bytehouse.settings.ByteHouseConfig;
import com.bytedance.bytehouse.settings.SettingKey;
import com.bytedance.bytehouse.stream.ValuesNativeInputFormat;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.time.Duration;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ByteHouseStatement implements SQLStatement {

    private static final Logger LOG = LoggerFactory.getLogger(ByteHouseStatement.class);

    private static final Pattern VALUES_REGEX = Pattern.compile("[V|v][A|a][L|l][U|u][E|e][S|s]\\s*\\(");
    private static final Pattern SELECT_DB_TABLE = Pattern.compile("(?i)FROM\\s+(\\S+\\.)?(\\S+)");

    private ResultSet lastResultSet;
    protected Block block;
    protected final ByteHouseConnection connection;
    protected final NativeContext nativeContext;

    private ByteHouseConfig cfg;
    private long maxRows;
    private String db;
    private String table = "unknown";

    private int updateCount = -1;
    private boolean isClosed = false;

    public ByteHouseStatement(ByteHouseConnection connection, NativeContext nativeContext) {
        this.connection = connection;
        this.nativeContext = nativeContext;
        this.cfg = connection.cfg();
        this.db = cfg.database();
    }

    @Override
    public boolean execute(String query) throws SQLException {
        return executeQuery(query) != null;
    }

    @Override
    public int executeUpdate(String query) throws SQLException {

        return ExceptionUtil.rethrowSQLException(() -> {
            extractDBAndTableName(query);
            Matcher matcher = VALUES_REGEX.matcher(query);

            if (matcher.find() && query.trim().toUpperCase(Locale.ROOT).startsWith("INSERT")) {
                lastResultSet = null;
                String insertQuery = query.substring(0, matcher.end() - 1);
                block = connection.getSampleBlock(insertQuery);
                block.initWriteBuffer();
                new ValuesNativeInputFormat(matcher.end() - 1, query).fill(block);
                updateCount = connection.sendInsertRequest(block);
                return updateCount;
            }
            updateCount = -1;
            QueryResult result = connection.sendQueryRequest(query, cfg);
            lastResultSet = new ByteHouseResultSet(this, cfg, db, table, result.header(), result.data());
            return 0;
        });
    }

    @Override
    public ResultSet executeQuery(String query) throws SQLException {
        executeUpdate(query);
        return getResultSet();
    }

    @Override
    public int getUpdateCount() throws SQLException {
        return updateCount;
    }

    @Override
    public ResultSet getResultSet() {
        return lastResultSet;
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        updateCount = -1;
        if (lastResultSet != null) {
            lastResultSet.close();
            lastResultSet = null;
        }
        return false;
    }

    @Override
    public void close() throws SQLException {
        LOG.debug("close Statement");
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
    public void setMaxRows(int max) throws SQLException {
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
    public void setQueryTimeout(int seconds) {
        this.cfg = cfg.withQueryTimeout(Duration.ofSeconds(seconds));
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return ResultSet.FETCH_FORWARD;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
    }

    @Override
    public int getFetchSize() throws SQLException {
        return 0;
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
    }

    @Override
    public boolean isPoolable() throws SQLException {
        return false;
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
    public ResultSet getGeneratedKeys() throws SQLException {
        return null;
    }

    @Override
    public Connection getConnection() {
        return connection;
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
    }

    @Override
    public void setCursorName(String name) throws SQLException {
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public Logger logger() {
        return ByteHouseStatement.LOG;
    }

    protected Block getSampleBlock(final String insertQuery) throws SQLException {
        return connection.getSampleBlock(insertQuery);
    }

    private void extractDBAndTableName(String sql) {
        String upperSQL = sql.trim().toUpperCase(Locale.ROOT);
        if (upperSQL.startsWith("SELECT")) {
            Matcher m = SELECT_DB_TABLE.matcher(sql);
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
