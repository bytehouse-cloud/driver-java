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

package com.bytedance.bytehouse.client;

import com.bytedance.bytehouse.buffer.SocketBuffedReader;
import com.bytedance.bytehouse.buffer.SocketBuffedWriter;
import com.bytedance.bytehouse.data.Block;
import com.bytedance.bytehouse.log.Logger;
import com.bytedance.bytehouse.log.LoggerFactory;
import com.bytedance.bytehouse.misc.Validate;
import com.bytedance.bytehouse.protocol.*;
import com.bytedance.bytehouse.serde.BinaryDeserializer;
import com.bytedance.bytehouse.serde.BinarySerializer;
import com.bytedance.bytehouse.stream.ClickHouseQueryResult;
import com.bytedance.bytehouse.stream.QueryResult;
import com.bytedance.bytehouse.settings.ClickHouseConfig;
import com.bytedance.bytehouse.settings.ClickHouseDefines;
import com.bytedance.bytehouse.settings.SettingKey;

import javax.net.ssl.*;
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

public class NativeClient {

    private static final Logger LOG = LoggerFactory.getLogger(NativeClient.class);

    /**
     * Connects to Gateway using either a secure (with TLS) or insecure TCP connection.
     * Equivalent code in driver-go can be found in dial() function of:
     * <a href="https://code.byted.org/bytehouse/driver-go/blob/main/conn/connect.go">connect.go</a>
     */
    public static NativeClient connect(ClickHouseConfig configure) throws SQLException {
        try {
            SocketAddress endpoint = new InetSocketAddress(configure.host(), configure.port());

            Socket socket = obtainSocket(configure);
            socket.setTcpNoDelay(configure.tcpNoDelay());
            socket.setSendBufferSize(ClickHouseDefines.SOCKET_SEND_BUFFER_BYTES);
            socket.setReceiveBufferSize(ClickHouseDefines.SOCKET_RECV_BUFFER_BYTES);
            socket.setKeepAlive(configure.tcpKeepAlive());
            socket.connect(endpoint, (int) configure.connectTimeout().toMillis());

            return new NativeClient(socket,
                    new BinarySerializer(new SocketBuffedWriter(socket), true),
                    new BinaryDeserializer(new SocketBuffedReader(socket), true));
        } catch (Exception ex) {
            throw new SQLException(ex);
        }
    }

    private static Socket obtainSocket(
            ClickHouseConfig configure
    ) throws NoSuchAlgorithmException, KeyManagementException, IOException {
        if (!configure.secure()) {
            // non-secure connection
            return new Socket();
        } else {
            // secure connection
            SSLSocketFactory sslSocketFactory;
            if (configure.skipVerification()) {
                // TrustManager that trusts all certificates. Used to skip TLS verification.
                TrustManager[] trustAllCertsManager = new TrustManager[] {new X509ExtendedTrustManager() {
                    @Override
                    public void checkClientTrusted(X509Certificate[] chain, String authType, Socket socket) { }

                    @Override
                    public void checkServerTrusted(X509Certificate[] chain, String authType, Socket socket) { }

                    @Override
                    public void checkClientTrusted(X509Certificate[] chain, String authType, SSLEngine engine) { }

                    @Override
                    public void checkServerTrusted(X509Certificate[] chain, String authType, SSLEngine engine) { }

                    @Override
                    public X509Certificate[] getAcceptedIssuers() {
                        return new X509Certificate[0];
                    }

                    @Override
                    public void checkClientTrusted(X509Certificate[] certs, String authType) { }

                    @Override
                    public void checkServerTrusted(X509Certificate[] certs, String authType) { }
                }};

                SSLContext context = SSLContext.getInstance("TLSv1.2");
                context.init(null, trustAllCertsManager, new SecureRandom());
                sslSocketFactory = context.getSocketFactory();
            } else {
                sslSocketFactory = (SSLSocketFactory) SSLSocketFactory.getDefault();
            }

            SSLSocket sslSocket = (SSLSocket) sslSocketFactory.createSocket();
            // Java 8 uses only TLS 1.2 by default. This line enables all supported protocols, including TLS 1.3
            sslSocket.setEnabledProtocols(sslSocket.getSupportedProtocols());

            return sslSocket;
        }
    }

    private final Socket socket;
    private final SocketAddress address;
    private final BinarySerializer serializer;
    private final BinaryDeserializer deserializer;

    public NativeClient(Socket socket, BinarySerializer serializer, BinaryDeserializer deserializer) {
        this.socket = socket;
        this.address = socket.getLocalSocketAddress();
        this.serializer = serializer;
        this.deserializer = deserializer;
    }

    public SocketAddress address() {
        return address;
    }

    public boolean ping(Duration soTimeout, NativeContext.ServerContext info) {
        try {
            sendRequest(PingRequest.INSTANCE);
            while (true) {
                Response response = receiveResponse(soTimeout, info);

                if (response instanceof PongResponse)
                    return true;

                // TODO there are some previous response we haven't consumed
                LOG.debug("expect pong, skip response: {}", response.type());
            }
        } catch (SQLException e) {
            LOG.warn(e.getMessage());
            return false;
        }
    }

    public Block receiveSampleBlock(Duration soTimeout, NativeContext.ServerContext info) throws SQLException {
        while (true) {
            Response response = receiveResponse(soTimeout, info);
            if (response instanceof DataResponse) {
                return ((DataResponse) response).block();
            }
            // TODO there are some previous response we haven't consumed
            LOG.debug("expect sample block, skip response: {}", response.type());
        }
    }

    public void sendHello(String client, long reversion, String db, String user, String password) throws SQLException {
        sendRequest(new HelloRequest(client, reversion, db, user, password));
    }

    public void sendQuery(String query, NativeContext.ClientContext info, Map<SettingKey, Serializable> settings) throws SQLException {
        sendQuery(UUID.randomUUID().toString(), QueryRequest.STAGE_COMPLETE, info, query, settings);
    }

    public void sendData(Block data) throws SQLException {
        sendRequest(new DataRequest("", data));
    }

    public HelloResponse receiveHello(Duration soTimeout, NativeContext.ServerContext info) throws SQLException {
        Response response = receiveResponse(soTimeout, info);
        Validate.isTrue(response instanceof HelloResponse, "Expect Hello Response.");
        return (HelloResponse) response;
    }

    public EOFStreamResponse receiveEndOfStream(Duration soTimeout, NativeContext.ServerContext info) throws SQLException {
        Response response = receiveResponse(soTimeout, info);
        Validate.isTrue(response instanceof EOFStreamResponse, "Expect EOFStream Response.");
        return (EOFStreamResponse) response;
    }

    public QueryResult receiveQuery(Duration soTimeout, NativeContext.ServerContext info) {
        return new ClickHouseQueryResult(() -> receiveResponse(soTimeout, info));
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
            throw new SQLException(ex.getMessage(), ex);
        }
    }

    private void sendQuery(String id, int stage, NativeContext.ClientContext info, String query,
                           Map<SettingKey, Serializable> settings) throws SQLException {
        sendRequest(new QueryRequest(id, info, stage, true, query, settings));
    }

    private void sendRequest(Request request) throws SQLException {
        try {
            LOG.trace("send request: {}", request.type());
            request.writeTo(serializer);
            serializer.flushToTarget(true);
        } catch (IOException ex) {
            throw new SQLException(ex.getMessage(), ex);
        }
    }

    private Response receiveResponse(Duration soTimeout, NativeContext.ServerContext info) throws SQLException {
        try {
            socket.setSoTimeout(((int) soTimeout.toMillis()));
            Response response = Response.readFrom(deserializer, info);
            LOG.trace("recv response: {}", response.type());
            return response;
        } catch (IOException ex) {
            throw new SQLException(ex.getMessage(), ex);
        }
    }
}