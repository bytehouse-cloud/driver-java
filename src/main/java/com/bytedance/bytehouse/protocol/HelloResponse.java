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

package com.bytedance.bytehouse.protocol;

import com.bytedance.bytehouse.serde.BinaryDeserializer;
import com.bytedance.bytehouse.settings.ByteHouseDefines;

import java.io.IOException;
import java.time.ZoneId;

public class HelloResponse implements Response {

    public static HelloResponse readFrom(BinaryDeserializer deserializer) throws IOException {
        String name = deserializer.readUTF8StringBinary();
        long majorVersion = deserializer.readVarInt();
        long minorVersion = deserializer.readVarInt();
        long serverReversion = deserializer.readVarInt();
        String serverTimeZone = getTimeZone(deserializer, serverReversion);
        String serverDisplayName = getDisplayName(deserializer, serverReversion);
        long serverVersionPatch = getVersionPatch(deserializer, serverReversion);

        return new HelloResponse(name, majorVersion, minorVersion, serverReversion,
                serverTimeZone, serverDisplayName, serverVersionPatch);
    }

    private static String getTimeZone(BinaryDeserializer deserializer, long serverReversion) throws IOException {
        return serverReversion >= ByteHouseDefines.DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE ?
                deserializer.readUTF8StringBinary() : ZoneId.systemDefault().getId();
    }

    private static String getDisplayName(BinaryDeserializer deserializer, long serverReversion) throws IOException {
        return serverReversion >= ByteHouseDefines.DBMS_MIN_REVISION_WITH_SERVER_DISPLAY_NAME ?
                deserializer.readUTF8StringBinary() : "localhost";
    }

    private static long getVersionPatch(BinaryDeserializer deserializer, long serverReversion) throws IOException {
        return serverReversion >= ByteHouseDefines.DBMS_MIN_REVISION_WITH_VERSION_PATCH ?
                deserializer.readVarInt() : 0;
    }

    private final long majorVersion;
    private final long minorVersion;
    private final long reversion;
    private final String serverName;
    private final String serverTimeZone;
    private final String serverDisplayName;
    private final long serverVersionPatch;

    public HelloResponse(
            String serverName, long majorVersion, long minorVersion, long reversion,
            String serverTimeZone,
            String serverDisplayName,
            long serverVersionPatch) {

        this.reversion = reversion;
        this.serverName = serverName;
        this.majorVersion = majorVersion;
        this.minorVersion = minorVersion;
        this.serverTimeZone = serverTimeZone;
        this.serverDisplayName = serverDisplayName;
        this.serverVersionPatch = serverVersionPatch;
    }

    @Override
    public ProtoType type() {
        return ProtoType.RESPONSE_HELLO;
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

    public String serverName() {
        return serverName;
    }

    public String serverTimeZone() {
        return serverTimeZone;
    }

    public String serverDisplayName() {
        return serverDisplayName;
    }

    public long serverVersionPatch() {
        return serverVersionPatch;
    }
}
