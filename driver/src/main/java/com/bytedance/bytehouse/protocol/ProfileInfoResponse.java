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
import java.io.IOException;

/**
 * Profile information from the server.
 */
public class ProfileInfoResponse implements Response {

    private final long rows;

    private final long blocks;

    private final long bytes;

    private final long appliedLimit;

    private final long rowsBeforeLimit;

    private final boolean calculatedRowsBeforeLimit;

    public ProfileInfoResponse(
            final long rows,
            final long blocks,
            final long bytes,
            final long appliedLimit,
            final long rowsBeforeLimit,
            final boolean calculatedRowsBeforeLimit
    ) {
        this.rows = rows;
        this.blocks = blocks;
        this.bytes = bytes;
        this.appliedLimit = appliedLimit;
        this.rowsBeforeLimit = rowsBeforeLimit;
        this.calculatedRowsBeforeLimit = calculatedRowsBeforeLimit;
    }

    public static ProfileInfoResponse readFrom(
            final BinaryDeserializer deserializer
    ) throws IOException {
        final long rows = deserializer.readVarInt();
        final long blocks = deserializer.readVarInt();
        final long bytes = deserializer.readVarInt();
        final long appliedLimit = deserializer.readVarInt();
        final long rowsBeforeLimit = deserializer.readVarInt();
        final boolean calculatedRowsBeforeLimit = deserializer.readBoolean();
        return new ProfileInfoResponse(
                rows,
                blocks,
                bytes,
                appliedLimit,
                rowsBeforeLimit,
                calculatedRowsBeforeLimit
        );
    }

    @Override
    public ProtoType type() {
        return ProtoType.RESPONSE_PROFILE_INFO;
    }

    public long rows() {
        return rows;
    }

    public long blocks() {
        return blocks;
    }

    public long bytes() {
        return bytes;
    }

    public long appliedLimit() {
        return appliedLimit;
    }

    public long rowsBeforeLimit() {
        return rowsBeforeLimit;
    }

    public boolean calculatedRowsBeforeLimit() {
        return calculatedRowsBeforeLimit;
    }
}
