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

import java.util.Map;
import javax.annotation.concurrent.Immutable;

/**
 * record class representing a service node.
 */
@Immutable
public class ServiceNode {

    private final String host;

    private final int port;

    private final Map<String, String> tags;

    // package private constructor makes it effectively final outside the package
    ServiceNode(final String host, final int port, final Map<String, String> tags) {
        this.host = host;
        this.port = port;
        this.tags = tags;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    @Override
    public String toString() {
        return "ServiceNode{" +
                "host='" + host + '\'' +
                ", port=" + port +
                ", tags=" + tags +
                '}';
    }
}
