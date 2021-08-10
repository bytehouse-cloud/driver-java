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

import com.bytedance.bytehouse.serde.BinarySerializer;
import com.bytedance.bytehouse.settings.BHConstants;
import java.io.IOException;

/**
 * A Context object describing who the client is.
 */
public class ClientContext {

    public static final int TCP_KINE = 1;

    public static final byte NO_QUERY = 0;

    public static final byte INITIAL_QUERY = 1;

    public static final byte SECONDARY_QUERY = 2;

    private final String clientName;

    private final String clientHostname;

    private final String initialAddress;

    public ClientContext(
            final String initialAddress,
            final String clientHostname,
            final String clientName) {
        this.clientName = clientName;
        this.clientHostname = clientHostname;
        this.initialAddress = initialAddress;
    }

    public void writeTo(final BinarySerializer serializer) throws IOException {
        serializer.writeVarInt(ClientContext.INITIAL_QUERY);
        serializer.writeUTF8StringBinary("");
        serializer.writeUTF8StringBinary("");
        serializer.writeUTF8StringBinary(initialAddress);

        // for TCP kind
        serializer.writeVarInt(TCP_KINE);
        serializer.writeUTF8StringBinary("");
        serializer.writeUTF8StringBinary(clientHostname);
        serializer.writeUTF8StringBinary(clientName);
        serializer.writeVarInt(BHConstants.MAJOR_VERSION);
        serializer.writeVarInt(BHConstants.MINOR_VERSION);
        serializer.writeVarInt(BHConstants.CLIENT_REVISION);
        serializer.writeUTF8StringBinary("");
        serializer.writeVarInt(BHConstants.CLIENT_REVISION); // might be versionPatch instead
    }
}
