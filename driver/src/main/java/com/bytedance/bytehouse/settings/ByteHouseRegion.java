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
package com.bytedance.bytehouse.settings;

/**
 * ByteHouse region details
 */
public enum ByteHouseRegion {
    INVALID("INVALID-REGION", "INVALID-REGION", 0),
    CN_NORTH_1("CN-NORTH-1", "gateway.aws-cn-north-1.bytehouse.cn", 19000),
    AP_SOUTHEAST_1("AP-SOUTHEAST-1", "gateway.aws-ap-southeast-1.bytehouse.cloud", 19000),
    BOE("BOE", "gateway.volc-boe.offline.bytehouse.cn", 19000),
    CN_BEIJING("CN-BEIJING", "bytehouse-cn-beijing.volces.com", 19000);

    private final String name;

    private final String host;

    private final int port;

    ByteHouseRegion(final String name, final String host, final int port) {
        this.name = name;
        this.host = host;
        this.port = port;
    }

    public static ByteHouseRegion fromString(String name) {
        for (ByteHouseRegion region : ByteHouseRegion.values()) {
            if (region.name.equals(name)) {
                return region;
            }
        }
        return INVALID;
    }

    public String getName() {
        return name;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    @Override
    public String toString() {
        return String.format("ByteHouseRegion{name=%s host=%s port=%d}", name, host, port);
    }
}
