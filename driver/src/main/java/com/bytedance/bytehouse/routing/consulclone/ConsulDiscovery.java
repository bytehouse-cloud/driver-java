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
package com.bytedance.bytehouse.routing.consulclone;

import com.bytedance.bytehouse.log.Logger;
import com.bytedance.bytehouse.log.LoggerFactory;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.annotation.concurrent.Immutable;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 * consul implementation. This class is a clone of
 * <a href="https://code.byted.org/inf/commons/blob/master/src/main/java/com/bytedance/commons/consul/Discovery.java">Discovery.class</a>
 * from the bytedance:common.
 * <br><br>
 * We copied this class out to avoid a bunch of irrelevant dependencies that come with it.
 */
public class ConsulDiscovery implements Discovery {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsulDiscovery.class);

    private static final String DEFAULT_AGENT_IP = "127.0.0.1";

    private static final String DEFAULT_AGENT_PORT = "2280";

    public String dataCenter;

    public String domain;

    private String agentIp;

    private String agentPort;

    /**
     * configurable constructor.
     */
    ConsulDiscovery(final String agentIp, final String agentPort) {
        this.agentIp = agentIp;
        this.agentPort = agentPort;
        this.dataCenter = "";
        this.domain = "";

        overrideAddr();
        getLocation();

        LOGGER.info("using consul at {}:{}", this.agentIp, this.agentPort);
    }

    /**
     * default factory for all default configurations.
     */
    ConsulDiscovery() {
        this(DEFAULT_AGENT_IP, DEFAULT_AGENT_PORT);
    }

    /**
     * 修复某些TCE环境下 http://127.0.0.1:2280 找不到consul agent的问题
     * 参考实现 https://code.byted.org/java_arch/consul-group/blob/master/consul-lang/src/main/java/com.bytedance/consul/DefaultConsulFactory.java
     * 以及sd命令的实现 /opt/tiger/toutiao/load/sd (进入TCE容器内查看sd脚本)
     * CONSUL_HOST = env.get("CONSUL_HTTP_HOST") or env.get("TCE_HOST_IP") or env.get("MY_HOST_IP")
     */
    private void overrideAddr() {
        if (Objects.equals(agentIp, DEFAULT_AGENT_IP)) {
            agentIp = Stream.<Supplier<String>>of(
                            () -> System.getenv("CONSUL_HTTP_HOST"),
                            () -> System.getenv("TCE_HOST_IP"),
                            () -> System.getenv("MY_HOST_IP"),
                            () -> System.getenv("MY_HOST_IPV6")
                    )
                    .map(Supplier::get)
                    .filter(Objects::nonNull)
                    .filter(s -> !s.isEmpty())
                    .findFirst()
                    .orElse(DEFAULT_AGENT_IP);
        }

        if (Objects.equals(agentPort, DEFAULT_AGENT_PORT)) {
            agentPort = Optional
                    .ofNullable(System.getenv("CONSUL_HTTP_PORT"))
                    .filter(Objects::nonNull)
                    .filter(s -> !s.isEmpty())
                    .orElse(DEFAULT_AGENT_PORT);
        }
    }

    /**
     * get localhost dc and domain.
     */
    private void getLocation() {
        try {
            final Response response = doGet(
                    "/v1/agent/self",
                    Collections.emptyMap(),
                    agentIp, agentPort
            );
            if (response.getStatusCode() == 200) {
                final String body = response.getBody();
                final JSONObject node = new JSONObject(body);
                if (node.has("Config")) {
                    final JSONObject config = node.getJSONObject("Config");
                    dataCenter = config.getString("Datacenter");
                    domain = config.get("Domain").toString();
                    dataCenter = dataCenter.substring(1, dataCenter.length() - 1);
                    domain = domain.substring(1, domain.length() - 1);
                }
            }
        } catch (Exception e) {
            LOGGER.error("", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<ServiceNode> lookupName(final String serviceName) {
        final String ipv4 = System.getenv().getOrDefault("BYTED_HOST_IP", "");
        final String ipv6 = System.getenv().getOrDefault("BYTED_HOST_IPV6", "");
        final Map<String, String> param = new HashMap<>();
        param.put("name", serviceName);

        if (!ipv4.isEmpty() && !ipv6.isEmpty()) {
            param.put("addr-family", "dual-stack");
            param.put("unique", "v6");
        } else if (!ipv6.isEmpty()) {
            param.put("addr-family", "v6");
        } else if (!ipv4.isEmpty()) {
            param.put("addr-family", "v4");
        }
        return lookupWithParam(param);
    }

    private List<ServiceNode> lookupWithParam(final Map<String, String> param) {
        try {
            final Response response = doGet("/v1/lookup/name", param, agentIp, agentPort);
            if (response.getStatusCode() == 200) {
                final String body = response.getBody();
                final JSONArray objects = new JSONArray(body);

                return StreamSupport.stream(objects.spliterator(), false)
                        .map(obj -> {
                            final JSONObject jsonObject = (JSONObject) obj;
                            final String host = jsonObject.getString("Host");
                            final int port = jsonObject.getInt("Port");
                            final Map<String, String> tags = jsonObject.getJSONObject("Tags")
                                    .toMap()
                                    .entrySet()
                                    .stream()
                                    .collect(Collectors.toMap(
                                            Map.Entry::getKey,
                                            entry -> entry.getValue().toString()
                                    ));

                            return new ServiceNode(host, port, Collections.unmodifiableMap(tags));
                        })
                        .collect(Collectors.toList());
            }
        } catch (Exception e) {
            LOGGER.error("", e);
        }
        return Collections.emptyList();
    }

    /**
     * a method to do post request.
     * <br><br>
     * The reason we are using {@link HttpURLConnection} is because we do not want to introduce
     * another http library that might cause the users of this jar to have conflicts.
     */
    public Response doGet(
            final String path,
            final Map<String, String> mapUrlParams,
            final String agentIp,
            final String agentPort
    ) {
        final String urlParams = mapUrlParams.entrySet()
                .stream()
                .map(pair -> pair.getKey() + "=" + pair.getValue())
                .collect(Collectors.joining("&"));

        String ip = agentIp;
        if (agentIp.contains(":")) {
            ip = "[" + agentIp + "]";
        }
        final String requestUrl = urlParams.isEmpty() ?
                ("http://" + ip + ":" + agentPort + path) :
                ("http://" + ip + ":" + agentPort + path + "?" + urlParams);

        try {
            final URL url = new URL(requestUrl);

            final HttpURLConnection conn = (HttpURLConnection) url.openConnection();

            conn.setRequestMethod("GET");
            conn.setRequestProperty("Content-Type", "application/json; charset=utf-8");
            conn.setRequestProperty("Accept", "application/json");

            conn.connect();

            final int responseCode = conn.getResponseCode();
            final String responseBody;

            try (InputStream inputStream = conn.getInputStream()) {
                final ByteArrayOutputStream result = new ByteArrayOutputStream(1024);
                final byte[] buffer = new byte[1024];
                for (int length; (length = inputStream.read(buffer)) != -1; ) {
                    result.write(buffer, 0, length);
                }
                responseBody = result.toString(StandardCharsets.UTF_8.name());
            }
            return new Response(responseCode, responseBody);
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException(e);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * representing the http response.
     */
    @Immutable
    private static final class Response {

        private final int statusCode;

        private final String body;

        /**
         * constructor.
         */
        public Response(final int statusCode, final String body) {
            this.statusCode = statusCode;
            this.body = body;
        }

        public int getStatusCode() {
            return statusCode;
        }

        public String getBody() {
            return body;
        }

        @Override
        public String toString() {
            return "Response{" +
                    "statusCode=" + statusCode +
                    ", body='" + body + '\'' +
                    '}';
        }
    }
}
