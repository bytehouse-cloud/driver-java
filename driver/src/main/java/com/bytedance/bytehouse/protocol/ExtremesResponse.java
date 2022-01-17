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

import com.bytedance.bytehouse.client.ServerContext;
import com.bytedance.bytehouse.data.Block;
import com.bytedance.bytehouse.serde.BinaryDeserializer;
import java.io.IOException;
import java.sql.SQLException;

/**
 * Identical to {@link DataResponse}.
 */
public class ExtremesResponse implements Response {

    private final String name;

    private final Block block;

    public ExtremesResponse(final String name, final Block block) {
        this.name = name;
        this.block = block;
    }

    public static ExtremesResponse readFrom(
            final BinaryDeserializer deserializer,
            final ServerContext info
    ) throws IOException, SQLException {
        final String name = deserializer.readUTF8StringBinary();

        deserializer.maybeEnableCompressed();
        final Block block = Block.readFrom(deserializer, info);
        deserializer.maybeDisableCompressed();

        return new ExtremesResponse(name, block);
    }

    @Override
    public ProtoType type() {
        return ProtoType.RESPONSE_EXTREMES;
    }

    public String name() {
        return name;
    }

    public Block block() {
        return block;
    }
}
