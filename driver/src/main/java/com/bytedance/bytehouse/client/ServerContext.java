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

import com.bytedance.bytehouse.protocol.HelloResponse;
import com.bytedance.bytehouse.settings.BHConstants;
import com.bytedance.bytehouse.settings.ByteHouseConfig;
import java.sql.SQLException;
import java.time.ZoneId;
import javax.annotation.concurrent.Immutable;

/**
 * A Context object describing who the server is
 */
@Immutable
public class ServerContext {

    private final long majorVersion;

    private final long minorVersion;

    private final long reversion;

    private final ZoneId timeZone;

    private final String displayName;

    private final long versionPatch;

    private final ByteHouseConfig configure;

    ServerContext(
            final long majorVersion,
            final long minorVersion,
            final long reversion,
            final ByteHouseConfig configure,
            final ZoneId timeZone,
            final String displayName,
            final long versionPatch
    ) {
        this.majorVersion = majorVersion;
        this.minorVersion = minorVersion;
        this.reversion = reversion;
        this.configure = configure;
        this.timeZone = timeZone;
        this.displayName = displayName;
        this.versionPatch = versionPatch;
    }

    /**
     * Factory method that creates {@link ServerContext}.
     */
    public static ServerContext create(
            final NativeClient nativeClient,
            final ByteHouseConfig configure
    ) throws SQLException {
        try {
            final long revision = BHConstants.CLIENT_REVISION;
            nativeClient.sendHello(
                    "client",
                    revision,
                    configure.database(),
                    configure.fullUsername(),
                    configure.password()
            );

            final HelloResponse response = nativeClient.receiveHello(
                    configure.queryTimeout(), null
            );
            final ZoneId timeZone = getZoneId(response.serverTimeZone());
            return new ServerContext(
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

    private static ZoneId getZoneId(String serverTimeZone) {
        if (serverTimeZone.equals("Local")) {
            return ZoneId.systemDefault();
        } else {
            return ZoneId.of(serverTimeZone);
        }
    }

    public long majorVersion() {
        return majorVersion;
    }

    public long minorVersion() {
        return minorVersion;
    }

    public long reversion() {
        return reversion;
    }

    public String version() {
        return majorVersion + "." + minorVersion + "." + reversion;
    }

    public ZoneId timeZone() {
        return timeZone;
    }

    public String displayName() {
        return displayName;
    }

    public long versionPatch() {
        return versionPatch;
    }

    public ByteHouseConfig getConfigure() {
        return configure;
    }
}
