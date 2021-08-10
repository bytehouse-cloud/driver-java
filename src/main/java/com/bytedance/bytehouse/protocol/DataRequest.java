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

import com.bytedance.bytehouse.data.Block;
import com.bytedance.bytehouse.serde.BinarySerializer;
import java.io.IOException;
import java.sql.SQLException;

/**
 * represents a {@link Block} to be sent to the server.
 */
public class DataRequest implements Request {

    public static final DataRequest EMPTY = new DataRequest("");

    private final String name;

    private final Block block;

    public DataRequest(final String name) {
        this(name, new Block());
    }

    public DataRequest(final String name, final Block block) {
        this.name = name;
        this.block = block;
    }

    @Override
    public ProtoType type() {
        return ProtoType.REQUEST_DATA;
    }

    @Override
    public void writeImpl(final BinarySerializer serializer) throws IOException, SQLException {
        serializer.writeUTF8StringBinary(name);

        serializer.maybeEnableCompressed();
        block.writeTo(serializer);
        serializer.maybeDisableCompressed();
    }
}
