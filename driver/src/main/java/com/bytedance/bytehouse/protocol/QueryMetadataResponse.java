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

/**
 * Query Metadata from the server.
 */
public class QueryMetadataResponse implements Response {

    private final String queryId;

    public QueryMetadataResponse(final String queryId) {
        this.queryId = queryId;
    }

    public static QueryMetadataResponse readFrom(
            final BinaryDeserializer deserializer
    ) throws IOException {
        return new QueryMetadataResponse(deserializer.readUTF8StringBinary());
    }

    @Override
    public ProtoType type() {
        return ProtoType.RESPONSE_QUERY_METADATA;
    }

    public String queryId() {
        return queryId;
    }
}
