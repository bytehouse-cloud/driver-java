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

import com.bytedance.bytehouse.serde.BinaryDeserializer;
import java.io.IOException;
import java.sql.SQLException;

/**
 * Table column response.
 */
public class TableColumnsResponse implements Response {

    private final String name;

    private final String description;

    public TableColumnsResponse(final String name, final String description) {
        this.name = name;
        this.description = description;
    }

    public static TableColumnsResponse readFrom(final BinaryDeserializer deserializer)
            throws IOException, SQLException {

        return new TableColumnsResponse(
                deserializer.readUTF8StringBinary(),
                deserializer.readUTF8StringBinary()
        );
    }

    @Override
    public ProtoType type() {
        return ProtoType.RESPONSE_TABLE_COLUMNS;
    }

    public String name() {
        return name;
    }

    public String description() {
        return description;
    }
}
