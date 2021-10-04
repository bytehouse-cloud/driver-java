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
package com.bytedance.bytehouse.settings;

public final class BHConstants {

    public static final String NAME = "ByteHouse";

    public static final String DEFAULT_CATALOG = "default";

    public static final String DEFAULT_DATABASE = "default";

    /**
     * Client version follows
     * <a href="https://code.byted.org/bytehouse/driver-go/blob/main/driver/lib/data/client_info.go">
     * client_info.go
     * </a>
     */
    public static final int MAJOR_VERSION = 0;

    public static final int MINOR_VERSION = 1;

    public static final int CLIENT_REVISION = 54406;

    // FIXME: 10/8/21 https://jira-sg.bytedance.net/browse/BYT-3118
    public static final int DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE = 54058;

    public static final int DBMS_MIN_REVISION_WITH_SERVER_DISPLAY_NAME = 54372;

    public static final int DBMS_MIN_REVISION_WITH_VERSION_PATCH = 54401;

    public static final int MAX_BLOCK_BYTES = 1024 * 1024 * 10;

    public static final int DATA_TYPE_CACHE_SIZE = 1024;

    public static final int COMPRESSION_HEADER_LENGTH = 9;

    public static final int CHECKSUM_LENGTH = 16;

    public static final int SOCKET_SEND_BUFFER_BYTES = 1024 * 1024;

    public static final int SOCKET_RECV_BUFFER_BYTES = 1024 * 1024;

    public static final int COLUMN_BUFFER_BYTES = 1024 * 1024;

    // For verification, MAX_INSERT_BLOCK_SIZE has been set to 1, default value would be decided
    // after performing benchmark (possible value can be 65536)
    public static final int MAX_INSERT_BLOCK_SIZE = 1;
}
