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

import com.bytedance.bytehouse.client.ServerContext;
import com.bytedance.bytehouse.data.Block;
import com.bytedance.bytehouse.serde.BinaryDeserializer;
import java.io.IOException;
import java.sql.SQLException;

/**
 * Represents server's application log from the server.
 */
public class LogResponse implements Response {

    private final String name;

    private final Block block;

    public LogResponse(String name, Block block) {
        this.name = name;
        this.block = block;
    }

    public static LogResponse readFrom(
            final BinaryDeserializer deserializer,
            final ServerContext info
    ) throws IOException, SQLException {

        final String name = deserializer.readUTF8StringBinary();

        deserializer.maybeDisableCompressed();
        final Block block = Block.readFrom(deserializer, info);
        deserializer.maybeEnableCompressed();

        return new LogResponse(name, block);
    }

    @Override
    public ProtoType type() {
        return ProtoType.RESPONSE_LOG;
    }

    public String name() {
        return name;
    }

    public Block block() {
        return block;
    }
}
