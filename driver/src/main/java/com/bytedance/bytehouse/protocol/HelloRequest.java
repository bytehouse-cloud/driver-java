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
package com.bytedance.bytehouse.protocol;

import com.bytedance.bytehouse.serde.BinarySerializer;
import com.bytedance.bytehouse.settings.BHConstants;
import java.io.IOException;

/**
 * Represents Hello to the server.
 */
public class HelloRequest implements Request {

    private final String clientName;

    private final long clientReversion;

    private final String defaultDatabase;

    private final String clientUsername;

    private final String clientPassword;

    public HelloRequest(
            final String clientName,
            final long clientReversion,
            final String defaultDatabase,
            final String clientUsername,
            final String clientPassword
    ) {
        this.clientName = clientName;
        this.clientReversion = clientReversion;
        this.defaultDatabase = defaultDatabase;
        this.clientUsername = clientUsername;
        this.clientPassword = clientPassword;
    }

    @Override
    public ProtoType type() {
        return ProtoType.REQUEST_HELLO;
    }

    @Override
    public void writeImpl(final BinarySerializer serializer) throws IOException {
        serializer.writeUTF8StringBinary(BHConstants.NAME + " " + clientName);
        serializer.writeVarInt(BHConstants.MAJOR_VERSION);
        serializer.writeVarInt(BHConstants.MINOR_VERSION);
        serializer.writeVarInt(clientReversion);
        serializer.writeUTF8StringBinary(defaultDatabase);
        serializer.writeUTF8StringBinary(clientUsername);
        serializer.writeUTF8StringBinary(clientPassword);
    }
}
