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

import com.bytedance.bytehouse.misc.AKSKTokenGeneratorWithJWT;
import com.bytedance.bytehouse.serde.BinarySerializer;
import com.bytedance.bytehouse.settings.BHConstants;
import java.io.IOException;

public class AKSKHelloRequest implements Request {
    private final String clientName;

    private final long clientReversion;

    private final String defaultDatabase;

    private final String clientAccessKey;

    private final String clientSecretKey;

    private final String currentDate;

    private final String region;

    private final String service;

    private final AKSKTokenGeneratorWithJWT tokenGenerator;

    public AKSKHelloRequest(
            final String clientName,
            final long clientReversion,
            final String defaultDatabase,
            final String clientAccessKey,
            final String clientSecretKey,
            final String currentDate,
            final String region,
            final String service,
            final AKSKTokenGeneratorWithJWT tokenGenerator
    ) {
        this.clientName = clientName;
        this.clientReversion = clientReversion;
        this.defaultDatabase = defaultDatabase;
        this.clientAccessKey = clientAccessKey;
        this.clientSecretKey = clientSecretKey;
        this.currentDate = currentDate;
        this.region = region;
        this.service = service;
        this.tokenGenerator = tokenGenerator;
    }

    @Override
    public Request.ProtoType type() {
        return Request.ProtoType.REQUEST_HELLO_AKSK;
    }

    @Override
    public void writeImpl(final BinarySerializer serializer) throws IOException {
        serializer.writeUTF8StringBinary(BHConstants.NAME + " " + clientName);
        serializer.writeVarInt(BHConstants.MAJOR_VERSION);
        serializer.writeVarInt(BHConstants.MINOR_VERSION);
        serializer.writeVarInt(clientReversion);
        serializer.writeUTF8StringBinary(defaultDatabase);
        serializer.writeUTF8StringBinary(scope());
        serializer.writeUTF8StringBinary(tokenGenerator.generate());
    }

    private String scope() {
        return String.join("/", this.clientAccessKey, this.currentDate, this.region, this.service);
    }
}
