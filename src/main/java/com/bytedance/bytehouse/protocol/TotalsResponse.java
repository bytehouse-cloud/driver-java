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
 * Total response.
 */
public class TotalsResponse implements Response {

    private final String name;

    private final Block block;

    TotalsResponse(final String name, final Block block) {
        this.name = name;
        this.block = block;
    }

    public static TotalsResponse readFrom(
            final BinaryDeserializer deserializer,
            final ServerContext info
    ) throws IOException, SQLException {
        return new TotalsResponse(
                deserializer.readUTF8StringBinary(),
                Block.readFrom(deserializer, info)
        );
    }

    @Override
    public ProtoType type() {
        return ProtoType.RESPONSE_TOTALS;
    }

    public String name() {
        return name;
    }

    public Block block() {
        return block;
    }
}
