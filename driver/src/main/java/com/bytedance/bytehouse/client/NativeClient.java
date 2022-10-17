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
package com.bytedance.bytehouse.client;

import com.bytedance.bytehouse.buffer.SocketBuffedReader;
import com.bytedance.bytehouse.buffer.SocketBuffedWriter;
import com.bytedance.bytehouse.data.Block;
import com.bytedance.bytehouse.log.Logger;
import com.bytedance.bytehouse.log.LoggerFactoryUtils;
import com.bytedance.bytehouse.misc.AKSKTokenGeneratorWithJWT;
import com.bytedance.bytehouse.misc.ValidateUtils;
import com.bytedance.bytehouse.protocol.AKSKHelloRequest;
import com.bytedance.bytehouse.protocol.DataRequest;
import com.bytedance.bytehouse.protocol.DataResponse;
import com.bytedance.bytehouse.protocol.EOFStreamResponse;
import com.bytedance.bytehouse.protocol.HelloRequest;
import com.bytedance.bytehouse.protocol.HelloResponse;
import com.bytedance.bytehouse.protocol.PingRequest;
import com.bytedance.bytehouse.protocol.PongResponse;
import com.bytedance.bytehouse.protocol.QueryRequest;
import com.bytedance.bytehouse.protocol.Request;
import com.bytedance.bytehouse.protocol.Response;
import com.bytedance.bytehouse.serde.BinaryDeserializer;
import com.bytedance.bytehouse.serde.BinarySerializer;
import com.bytedance.bytehouse.settings.BHConstants;
import com.bytedance.bytehouse.settings.ByteHouseConfig;
import com.bytedance.bytehouse.settings.SettingKey;
import com.bytedance.bytehouse.stream.ByteHouseQueryResult;
import com.bytedance.bytehouse.stream.QueryResult;
import java.io.IOException;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509ExtendedTrustManager;

/**
 * the client that handles low level connection details with the server, over TCP.
 * <br><br>
 * This class owns the {@link Socket} as well as the underlying {@link java.io.InputStream}
 * and {@link java.io.OutputStream}. Hence it needs to close them.
 */
public class NativeClient implements AutoCloseable {

    private static final Logger LOG = LoggerFactoryUtils.getLogger(NativeClient.class);

    private final Socket socket;

    private final SocketAddress address;

    private final BinarySerializer serializer;

    private final BinaryDeserializer deserializer;

    public NativeClient(
            final Socket socket,
            final BinarySerializer serializer,
            final BinaryDeserializer deserializer
    ) {
        this.socket = socket;
        this.address = socket.getLocalSocketAddress();
        this.serializer = serializer;
        this.deserializer = deserializer;
    }

    public static NativeClient connect(final ByteHouseConfig configure) throws SQLException {
        try {
            final SocketAddress endpoint = new InetSocketAddress(
                    configure.host(), configure.port()
            );

            final Socket socket = obtainSocket(configure);
            socket.setTcpNoDelay(configure.tcpNoDelay());
            socket.setSendBufferSize(BHConstants.SOCKET_SEND_BUFFER_BYTES);
            socket.setReceiveBufferSize(BHConstants.SOCKET_RECV_BUFFER_BYTES);
            socket.setKeepAlive(configure.tcpKeepAlive());
            socket.connect(endpoint, (int) configure.connectTimeout().toMillis());

            // this sets the data compression boolean for the entire connection. If enableCompression = true, all Blocks
            // exchanged during the connection should be compressed. enableCompression can be changed via method
            // setEnableCompression()
            final boolean enableCompression = configure.enableCompression();

            return new NativeClient(
                    socket,
                    new BinarySerializer(new SocketBuffedWriter(socket), enableCompression),
                    new BinaryDeserializer(new SocketBuffedReader(socket), enableCompression)
            );
        } catch (Exception ex) {
            throw new SQLException(ex);
        }
    }

    private static Socket obtainSocket(
            final ByteHouseConfig configure
    ) throws NoSuchAlgorithmException, KeyManagementException, IOException {
        if (!configure.secure()) {
            // non-secure connection
            return new Socket();
        } else {
            // secure connection
            final SSLSocketFactory sslSocketFactory;
            if (configure.skipVerification()) {
                // TrustManager that trusts all certificates. Used to skip TLS verification.
                TrustManager[] trustAllCertsManager = new TrustManager[]{new X509ExtendedTrustManager() {
                    @Override
                    public void checkClientTrusted(X509Certificate[] chain, String authType, Socket socket) {
                    }

                    @Override
                    public void checkServerTrusted(X509Certificate[] chain, String authType, Socket socket) {
                    }

                    @Override
                    public void checkClientTrusted(X509Certificate[] chain, String authType, SSLEngine engine) {
                    }

                    @Override
                    public void checkServerTrusted(X509Certificate[] chain, String authType, SSLEngine engine) {
                    }

                    @Override
                    public X509Certificate[] getAcceptedIssuers() {
                        return new X509Certificate[0];
                    }

                    @Override
                    public void checkClientTrusted(X509Certificate[] certs, String authType) {
                    }

                    @Override
                    public void checkServerTrusted(X509Certificate[] certs, String authType) {
                    }
                }};

                final SSLContext context = SSLContext.getInstance("TLSv1.2");
                context.init(null, trustAllCertsManager, new SecureRandom());
                sslSocketFactory = context.getSocketFactory();
            } else {
                sslSocketFactory = (SSLSocketFactory) SSLSocketFactory.getDefault();
            }

            final SSLSocket sslSocket = (SSLSocket) sslSocketFactory.createSocket();
            // Java 8 uses only TLS 1.2 by default. This line enables all supported protocols, including TLS 1.3
//            sslSocket.setEnabledProtocols(sslSocket.getSupportedProtocols());

            return sslSocket;
        }
    }

    /**
     * Set enableCompression boolean on this NativeClient.
     */
    public void setEnableCompression(final boolean enableCompression) {
        serializer.setEnableCompression(enableCompression);
        deserializer.setEnableCompression(enableCompression);
    }

    public SocketAddress address() {
        return address;
    }

    public boolean ping(
            final Duration soTimeout,
            final ServerContext info
    ) {
        try {
            sendRequest(PingRequest.INSTANCE);
            while (!Thread.currentThread().isInterrupted()) {
                final Response response = receiveResponse(soTimeout, info);

                // seek only the PongResponse
                if (response instanceof PongResponse)
                    return true;

                LOG.debug("expect pong, skip response: {}", response.type());
            }
        } catch (SQLException e) {
            LOG.warn(e.getMessage());
            return false;
        }
        LOG.warn("Pinging server is interrupted");
        return false;
    }

    /**
     * Get metadata for an insert query.
     */
    public Block receiveSampleBlock(
            final Duration soTimeout,
            final ServerContext serverContext
    ) throws SQLException {
        while (!Thread.currentThread().isInterrupted()) {
            final Response response = receiveResponse(soTimeout, serverContext);
            if (response instanceof DataResponse) {
                return ((DataResponse) response).block();
            }
            // TODO there are some previous response we haven't consumed
            LOG.debug("expect sample block, skip response: {}", response.type());
        }
        return Block.empty();
    }

    public void sendHello(
            final String client,
            final long reversion,
            final String db,
            final String user,
            final String password
    ) throws SQLException {
        sendRequest(new HelloRequest(client, reversion, db, user, password));
    }

    public void sendHelloAKSK(
            final String client,
            final long reversion,
            final String db,
            final String accessKey,
            final String secretKey,
            final String date,
            final String region,
            final String service,
            final AKSKTokenGeneratorWithJWT tokenGenerator
            ) throws SQLException {
        sendRequest(new AKSKHelloRequest(client, reversion, db, accessKey, secretKey, date, region, service, tokenGenerator));
    }

    public void sendQuery(
            final String query,
            final ClientContext info,
            final Map<SettingKey, Serializable> settings,
            final boolean enableCompression
    ) throws SQLException {
        sendQuery(
                UUID.randomUUID().toString(),
                QueryRequest.STAGE_COMPLETE,
                info,
                query,
                settings,
                enableCompression
        );
    }

    public void sendData(final Block data) throws SQLException {
        sendRequest(new DataRequest("", data));
    }

    public HelloResponse receiveHello(
            final Duration soTimeout,
            final ServerContext info
    ) throws SQLException {
        Response response = receiveResponse(soTimeout, info);
        ValidateUtils.isTrue(response instanceof HelloResponse, "Expect Hello Response.");
        return (HelloResponse) response;
    }

    public EOFStreamResponse receiveEndOfStream(
            final Duration soTimeout,
            final ServerContext info
    ) throws SQLException {
        final Response response = receiveResponse(soTimeout, info);
        ValidateUtils.isTrue(
                response instanceof EOFStreamResponse,
                "Expect EOFStream Response."
        );
        return (EOFStreamResponse) response;
    }

    public QueryResult receiveQuery(
            final Duration soTimeout,
            final ServerContext info
    ) {
        return new ByteHouseQueryResult(() -> receiveResponse(soTimeout, info));
    }

    public void silentDisconnect() {
        try {
            disconnect();
        } catch (Throwable th) {
            LOG.debug("disconnect throw exception.", th);
        }
    }

    public void disconnect() throws SQLException {
        try {
            if (socket.isClosed()) {
                LOG.info("socket already closed, ignore");
                return;
            }
            LOG.trace("flush and close socket");
            serializer.flushToTarget(true);
            socket.close();
        } catch (IOException ex) {
            throw new SQLException(ex);
        }
    }

    private void sendQuery(
            final String id,
            final int stage,
            final ClientContext info,
            final String query,
            final Map<SettingKey, Serializable> settings,
            final boolean enableCompression
    ) throws SQLException {
        sendRequest(new QueryRequest(id, info, stage, enableCompression, query, settings));
    }

    private void sendRequest(final Request request) throws SQLException {
        try {
            LOG.trace("send request: {}", request.type());
            request.writeTo(serializer);
            serializer.flushToTarget(true);
        } catch (IOException ex) {
            throw new SQLException(ex);
        }
    }

    private Response receiveResponse(
            final Duration soTimeout,
            final ServerContext info
    ) throws SQLException {
        try {
            socket.setSoTimeout(((int) soTimeout.toMillis()));
            final Response response = Response.readFrom(deserializer, info);
            LOG.trace("recv response: {}", response.type());
            return response;
        } catch (IOException ex) {
            throw new SQLException(ex);
        }
    }

    @Override
    public void close() throws SQLException {
        disconnect();
    }
}
