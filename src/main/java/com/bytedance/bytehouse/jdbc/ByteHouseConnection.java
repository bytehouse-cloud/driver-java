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

import com.bytedance.bytehouse.client.NativeClient;
import com.bytedance.bytehouse.client.NativeContext;
import com.bytedance.bytehouse.client.SessionState;
import com.bytedance.bytehouse.data.Block;
import com.bytedance.bytehouse.data.DataTypeFactory;
import com.bytedance.bytehouse.exception.ByteHouseSQLException;
import com.bytedance.bytehouse.jdbc.statement.ByteHousePreparedInsertStatement;
import com.bytedance.bytehouse.jdbc.statement.ByteHousePreparedQueryStatement;
import com.bytedance.bytehouse.jdbc.statement.ByteHouseStatement;
import com.bytedance.bytehouse.jdbc.wrapper.BHConnection;
import com.bytedance.bytehouse.log.Logger;
import com.bytedance.bytehouse.log.LoggerFactory;
import com.bytedance.bytehouse.misc.Validate;
import com.bytedance.bytehouse.protocol.HelloResponse;
import com.bytedance.bytehouse.settings.BHConstants;
import com.bytedance.bytehouse.settings.ByteHouseConfig;
import com.bytedance.bytehouse.settings.ByteHouseErrCode;
import com.bytedance.bytehouse.settings.SettingKey;
import com.bytedance.bytehouse.stream.QueryResult;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.sql.Array;
import java.sql.ClientInfoStatus;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.sql.Struct;
import java.time.Duration;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

/**
 * Provides implementation for {@link BHConnection}.
 */
public class ByteHouseConnection implements BHConnection {

    private static final Logger LOG = LoggerFactory.getLogger(ByteHouseConnection.class);

    private static final Pattern VALUES_REGEX = Pattern.compile("[Vv][Aa][Ll][Uu][Ee][Ss]\\s*\\(");

    private final AtomicBoolean isClosed;

    private final AtomicReference<ByteHouseConfig> cfg;

    private final AtomicReference<SessionState> state = new AtomicReference<>(SessionState.IDLE);

    private volatile NativeContext nativeCtx;

    protected ByteHouseConnection(
            final ByteHouseConfig cfg,
            final NativeContext nativeCtx
    ) {
        this.isClosed = new AtomicBoolean(false);
        this.cfg = new AtomicReference<>(cfg);
        this.nativeCtx = nativeCtx;
    }

    /**
     * Creates {@link ByteHouseConnection}.
     */
    public static ByteHouseConnection createByteHouseConnection(
            final ByteHouseConfig configure
    ) throws SQLException {
        return new ByteHouseConnection(configure, createNativeContext(configure));
    }

    private static NativeContext createNativeContext(
            final ByteHouseConfig configure
    ) throws SQLException {
        final NativeClient nativeClient = NativeClient.connect(configure);
        return new NativeContext(
                clientContext(nativeClient, configure),
                serverContext(nativeClient, configure),
                nativeClient
        );
    }

    private static NativeContext.ClientContext clientContext(
            final NativeClient nativeClient,
            final ByteHouseConfig configure
    ) throws SQLException {
        Validate.isTrue(nativeClient.address() instanceof InetSocketAddress);
        final InetSocketAddress address = (InetSocketAddress) nativeClient.address();
        final String clientName = String.format(Locale.ROOT, "%s %s",
                BHConstants.NAME, "client");
        final String initialAddress = "[::ffff:127.0.0.1]:0";
        return new NativeContext.ClientContext(initialAddress, address.getHostName(), clientName);
    }

    private static NativeContext.ServerContext serverContext(
            final NativeClient nativeClient,
            final ByteHouseConfig configure
    ) throws SQLException {
        try {
            final long revision = BHConstants.CLIENT_REVISION;
            nativeClient.sendHello("client", revision, configure.database(),
                    configure.fullUsername(), configure.password());

            final HelloResponse response = nativeClient.receiveHello(
                    configure.queryTimeout(), null
            );
            final ZoneId timeZone = getZoneId(response.serverTimeZone());
            return new NativeContext.ServerContext(
                    response.majorVersion(),
                    response.minorVersion(),
                    response.reversion(),
                    configure,
                    timeZone,
                    response.serverDisplayName(),
                    response.serverVersionPatch()
            );
        } catch (SQLException rethrows) {
            nativeClient.silentDisconnect();
            throw rethrows;
        }
    }

    /**
     * Temporary fix for CNCH returning "Local" as a serverTimeZone.
     */
    private static ZoneId getZoneId(String serverTimeZone) {
        if (serverTimeZone.equals("Local")) {
            return ZoneId.systemDefault();
        } else {
            return ZoneId.of(serverTimeZone);
        }
    }

    public ByteHouseConfig cfg() {
        return cfg.get();
    }

    public NativeContext.ServerContext serverContext() {
        return nativeCtx.serverCtx();
    }

    public NativeContext.ClientContext clientContext() {
        return nativeCtx.clientCtx();
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        LOG.warn("Transaction is not supported in Bytehouse");
        return true;
    }

    /**
     * autoCommit is always true as transactions are not supported in ByteHouse.
     */
    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        LOG.warn("Transaction is not supported in Bytehouse");
        if (!autoCommit) {
            throw new SQLFeatureNotSupportedException("Transaction is not supported in Bytehouse");
        }
    }

    /**
     * autoCommit is always true, so commit will always throw SQLException.
     */
    @Override
    public void commit() throws SQLException {
        throw new SQLFeatureNotSupportedException("Transaction is not supported in Bytehouse");
    }

    /**
     * autoCommit is always true, so rollback will always throw SQLException.
     */
    @Override
    public void rollback() throws SQLException {
        throw new SQLFeatureNotSupportedException("Transaction is not supported in Bytehouse");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isReadOnly() throws SQLException {
        return false;
    }

    /**
     * read-only mode is not supported.
     */
    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        // FIXME: 10/8/21 https://jira-sg.bytedance.net/browse/BYT-3098
        if (readOnly) {
            throw new SQLFeatureNotSupportedException("read-only mode is not supported");
        }
    }

    @Override
    public int getHoldability() throws SQLException {
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    /**
     * holdability cannot be changed from ResultSet.CLOSE_CURSORS_AT_COMMIT.
     * Transactions are not supported, so holdability has no effect.
     */
    @Override
    public void setHoldability(final int holdability) throws SQLException {
        if (holdability != ResultSet.CLOSE_CURSORS_AT_COMMIT) {
            throw new SQLFeatureNotSupportedException("given holdability is not supported");
        }
    }

    @Override
    public void abort(final Executor executor) throws SQLException {
        this.close();
    }

    @Override
    public void close() throws SQLException {
        if (!isClosed() && isClosed.compareAndSet(false, true)) {
            NativeClient nativeClient = nativeCtx.nativeClient();
            nativeClient.disconnect();
        }
    }

    @Override
    public boolean isClosed() {
        return isClosed.get();
    }

    @Override
    public Statement createStatement() throws SQLException {
        Validate.isTrue(!isClosed(), "Unable to create Statement, "
                + "because the connection is closed.");
        return new ByteHouseStatement(this, nativeCtx);
    }

    @Override
    public PreparedStatement prepareStatement(String query) throws SQLException {
        // FIXME: 10/8/21 https://jira-sg.bytedance.net/browse/BYT-3099
        Validate.isTrue(!isClosed(), "Unable to create PreparedStatement, "
                + "because the connection is closed.");
        final Matcher matcher = VALUES_REGEX.matcher(query);
        if (matcher.find()) {
            return new ByteHousePreparedInsertStatement(
                    matcher.end() - 1,
                    query,
                    this,
                    nativeCtx
            );
        } else {
            return new ByteHousePreparedQueryStatement(this, nativeCtx, query);
        }
    }

    /**
     * Use DatabaseMetaData.getClientInfoProperties() to retrieve client info properties supported.
     * Currently no properties are supported.
     */
    @Override
    public void setClientInfo(final Properties properties) throws SQLClientInfoException {
        final Map<String, ClientInfoStatus> failed = new HashMap<>();
        for (final Map.Entry<Object, Object> entry : properties.entrySet()) {
            failed.put((String) entry.getKey(), ClientInfoStatus.REASON_UNKNOWN_PROPERTY);
        }
        throw new SQLClientInfoException(failed);
    }

    @Override
    public void setClientInfo(
            final String name,
            final String value
    ) throws SQLClientInfoException {
        final Properties properties = new Properties();
        properties.put(name, value);
        this.setClientInfo(properties);
    }

    @Override
    public Array createArrayOf(
            final String typeName,
            final Object[] elements
    ) throws SQLException {
        Validate.isTrue(!isClosed(), "Unable to create Array, "
                + "because the connection is closed.");
        return new ByteHouseArray(DataTypeFactory.get(typeName, nativeCtx.serverCtx()), elements);
    }

    @Override
    public Struct createStruct(
            final String typeName,
            final Object[] attributes
    ) throws SQLException {
        Validate.isTrue(!isClosed(), "Unable to create Struct, "
                + "because the connection is closed.");
        return new ByteHouseStruct(typeName, attributes);
    }

    @Override
    public boolean isValid(int timeout) {
        return getNativeClient().ping(Duration.ofSeconds(timeout), nativeCtx.serverCtx());
    }

    @Override
    @Nullable
    public String getSchema() {
        return this.cfg().database();
    }

    // ByteHouse support only `database`, we treat it as JDBC `schema`
    @Override
    public void setSchema(final String schema) {
        this.cfg.set(this.cfg().withDatabase(schema));
    }

    @Override
    public String getCatalog() {
        return null;
    }

    @Override
    public void setCatalog(final String catalog) {
        // do nothing
    }

    @Override
    public int getTransactionIsolation() {
        return Connection.TRANSACTION_NONE;
    }

    /**
     * Transactions are not supported in ByteHouse.
     */
    @Override
    public void setTransactionIsolation(final int level) throws SQLException {
        throw new ByteHouseSQLException(
                ByteHouseErrCode.UNSUPPORTED_METHOD.code(),
                "Transaction is not supported in Bytehouse");
    }

    @Override
    public SQLWarning getWarnings() {
        return null;
    }

    @Override
    public void clearWarnings() {
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        return new ByteHouseDatabaseMetadata(cfg().jdbcUrl(), this);
    }

    @Override
    public Logger logger() {
        return ByteHouseConnection.LOG;
    }
    // when sendInsertRequest we must ensure the connection is healthy
    // the #getSampleBlock() must be called before this method

    @Override
    public boolean getEnableCompression() {
        return this.cfg().enableCompression();
    }

    @Override
    public void setEnableCompression(final boolean enableCompression) {
        getNativeClient().setEnableCompression(enableCompression);
        this.cfg.set(this.cfg().withEnableCompression(enableCompression));
    }

    @Override
    public boolean ping(final Duration timeout) {
        return nativeCtx.nativeClient().ping(timeout, nativeCtx.serverCtx());
    }

    /**
     * Get Sample block.
     */
    public Block getSampleBlock(final String insertQuery) throws SQLException {
        NativeClient nativeClient = getHealthyNativeClient();
        nativeClient.sendQuery(insertQuery, nativeCtx.clientCtx(), cfg.get().settings(), cfg.get().enableCompression());
        Validate.isTrue(this.state.compareAndSet(SessionState.IDLE, SessionState.WAITING_INSERT),
                "Connection is currently waiting for an insert operation, check your previous InsertStatement.");
        return nativeClient.receiveSampleBlock(cfg.get().queryTimeout(), nativeCtx.serverCtx());
    }

    /**
     * Used by Statement objects to send and receive queries using this connection.
     */
    public QueryResult sendQueryRequest(
            final String query,
            final ByteHouseConfig cfg
    ) throws SQLException {
        Validate.isTrue(this.state.get() == SessionState.IDLE,
                "Connection is currently waiting for an insert operation, "
                        + "check your previous InsertStatement.");
        final NativeClient nativeClient = getHealthyNativeClient();

        // enableCompression is a Connection level parameter, so it is obtained from this.cfg
        boolean enableCompression = this.cfg.get().enableCompression();

        // query settings and queryTimeout can be altered by Statement objects,
        // so they are obtained from cfg parameter
        final Map<SettingKey, Serializable> settings = cfg.settings();
        final Duration queryTimeout = cfg.queryTimeout();

        nativeClient.sendQuery(query, nativeCtx.clientCtx(), settings, enableCompression);
        return nativeClient.receiveQuery(queryTimeout, nativeCtx.serverCtx());
    }

    /**
     * send insert request.
     */
    public int sendInsertRequest(final Block block) throws SQLException {
        Validate.isTrue(this.state.get() == SessionState.WAITING_INSERT,
                "Call getSampleBlock before insert.");

        NativeClient nativeClient = getNativeClient();
        nativeClient.sendData(block);
        nativeClient.sendData(new Block());
        nativeClient.receiveEndOfStream(cfg.get().queryTimeout(), nativeCtx.serverCtx());
        Validate.isTrue(this.state.compareAndSet(SessionState.WAITING_INSERT, SessionState.IDLE));
        return block.rowCnt();
    }

    synchronized private NativeClient getHealthyNativeClient() throws SQLException {
        final NativeContext oldCtx = nativeCtx;
        if (!oldCtx.nativeClient().ping(cfg.get().queryTimeout(), nativeCtx.serverCtx())) {
            LOG.warn(
                    "connection loss with state[{}], create new connection and reset state",
                    state
            );
            nativeCtx = createNativeContext(cfg.get());
            state.set(SessionState.IDLE);
            oldCtx.nativeClient().silentDisconnect();
        }

        return nativeCtx.nativeClient();
    }

    private NativeClient getNativeClient() {
        return nativeCtx.nativeClient();
    }
}
