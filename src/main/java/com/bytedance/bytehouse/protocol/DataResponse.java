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

import com.bytedance.bytehouse.client.NativeContext;
import com.bytedance.bytehouse.data.Block;
import com.bytedance.bytehouse.serde.BinaryDeserializer;
import java.io.IOException;
import java.sql.SQLException;

/**
 * represents a {@link Block} from the server.
 */
public class DataResponse implements Response {

    private final String name;

    private final Block block;

    public DataResponse(final String name, final Block block) {
        this.name = name;
        this.block = block;
    }

    public static DataResponse readFrom(
            final BinaryDeserializer deserializer,
            final NativeContext.ServerContext info
    ) throws IOException, SQLException {

        final String name = deserializer.readUTF8StringBinary();

        deserializer.maybeEnableCompressed();
        final Block block = Block.readFrom(deserializer, info);
        deserializer.maybeDisableCompressed();

        return new DataResponse(name, block);
    }

    @Override
    public ProtoType type() {
        return ProtoType.RESPONSE_DATA;
    }

    public String name() {
        return name;
    }

    public Block block() {
        return block;
    }
}
