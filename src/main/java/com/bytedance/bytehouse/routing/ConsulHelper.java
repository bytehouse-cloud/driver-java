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
package com.bytedance.bytehouse.routing;

import com.bytedance.bytehouse.exception.ByteHouseClientException;
import com.bytedance.bytehouse.log.Logger;
import com.bytedance.bytehouse.log.LoggerFactory;
import com.bytedance.bytehouse.routing.consulclone.Discovery;
import com.bytedance.bytehouse.routing.consulclone.ServiceNode;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Provides helper functions for service discovery through consul.
 */
public class ConsulHelper {

    private static final Logger log = LoggerFactory.getLogger(ConsulHelper.class);

    private static final String CONSUL_MARKER = "consul:";

    private static final Pattern CONSUL_PATTERN =
            Pattern.compile(CONSUL_MARKER + "[a-zA-Z0-9_.\\-]+:[a-zA-Z0-9_.\\-]+");

    private static final String TAG_CLUSTER = "cluster";

    private static final String TAG_PORT2 = "PORT2";

    private static final String TAG_PORT1 = "PORT1";

    private static final String TAG_K8S_SERVICE_ADDR = "hostname";

    private final Discovery discovery;

    /**
     * Creates ConsulHelper with consul discovery object.
     */
    public ConsulHelper(final Discovery discovery) {
        this.discovery = discovery;
    }

    /**
     * Discovers list of valid consul service nodes that matches the consulUrl
     * on both the serviceName and clusterTag.
     */
    @SuppressWarnings("PMD.AvoidCatchingGenericException")
    public List<ServiceNode> discoverServiceNodes(final String consulUrl) {
        if (!CONSUL_PATTERN.matcher(consulUrl).matches()) {
            throw new IllegalArgumentException("Invalid format for consul discovery. Expect "
                    + "consul:${serviceName}:${clusterTag} but got " + consulUrl);
        }

        final String[] consulUrlParts = consulUrl.split(":");
        final String serviceName = consulUrlParts[1];

        final List<ServiceNode> allDiscoveredNodes;
        try {
            allDiscoveredNodes = discovery.lookupName(serviceName);
        } catch (Exception e) {
            throw new ByteHouseClientException(e);
        }

        final String clusterTag = consulUrlParts[2];
        final List<ServiceNode> uniqueClusterNodes = new ArrayList<>();
        final Set<String> uniqueIps = new HashSet<>();

        for (final ServiceNode node : allDiscoveredNodes) {
            if (getClusterFromNode(node).equals(clusterTag)) {
                final String ipv4 = getIpv4FromNode(node);
                if (uniqueIps.add(ipv4)) {
                    uniqueClusterNodes.add(node);
                } else {
                    log.error("Consul returns duplicated ip: {}. ignoring...", ipv4);
                }
            }
        }
        return uniqueClusterNodes;
    }

    /**
     * Returns whether url should be used for consul discovery.
     */
    public boolean isConsulUrl(final String url) {
        return url.startsWith(CONSUL_MARKER);
    }

    /**
     * Obtains IPV4 address from {@link ServiceNode} object.
     */
    public String getIpv4FromNode(final ServiceNode node) {
        return Objects.requireNonNull(node.getHost(),
                "ipv4 is missing from ServiceNode object");
    }

    /**
     * Obtains port number from {@link ServiceNode} object.
     */
    public Integer getHttpPortFromNode(final ServiceNode node) {
        final String port = Objects.requireNonNull(node.getTags().get(TAG_PORT2),
                "port is missing from ServiceNode object");
        return Integer.valueOf(port);
    }

    /**
     * Obtains port number from {@link ServiceNode} object.
     */
    public Integer getTcpPortFromNode(final ServiceNode node) {
        return node.getPort();
    }

    /**
     * Obtains the human-readable k8s service address from {@link ServiceNode} object.
     */
    public String getK8sServiceAddr(final ServiceNode node) {
        return node.getTags().getOrDefault(TAG_K8S_SERVICE_ADDR, "");
    }

    /**
     * Obtains the cluster name from {@link ServiceNode} object.
     */
    public String getClusterFromNode(final ServiceNode node) {
        return Objects.requireNonNull(node.getTags().get(TAG_CLUSTER),
                "cluster is missing from ServiceNode object");
    }
}
