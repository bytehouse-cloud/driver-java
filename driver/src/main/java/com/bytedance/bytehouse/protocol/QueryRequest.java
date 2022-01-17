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

import com.bytedance.bytehouse.client.ClientContext;
import com.bytedance.bytehouse.serde.BinarySerializer;
import com.bytedance.bytehouse.serde.SettingType;
import com.bytedance.bytehouse.settings.SettingKey;
import java.io.IOException;
import java.io.Serializable;
import java.sql.SQLException;
import java.util.Map;

/**
 * Query request to the server
 */
public class QueryRequest implements Request {

    // Only read/have been read the columns specified in the query.
    public static final int STAGE_FETCH_COLUMNS = 0;

    // Until the stage where the results of processing on different servers can be combined.
    public static final int STAGE_WITH_MERGEABLE_STATE = 1;

    // Completely.
    public static final int STAGE_COMPLETE = 2;

    // Until the stage where the aggregate functions were calculated and finalized.
    // It is used for auto distributed_group_by_no_merge optimization for distributed engine.
    // (See comments in StorageDistributed).
    public static final int STAGE_WITH_MERGEABLE_STATE_AFTER_AGGREGATION = 3;

    private final int stage;

    private final String queryId;

    private final String queryString;

    /**
     * this enableCompression boolean should be equal to the enableCompression boolean for the entire connection,
     * set in the ByteHouseConfig object. If set to true, query DataResponse sent by server will be compressed and
     * insert DataRequest sent by driver should be compressed too.
     */
    private final boolean enableCompression;

    private final ClientContext clientContext;

    private final Map<SettingKey, Serializable> settings;

    public QueryRequest(
            final String queryId,
            final ClientContext clientContext,
            final int stage,
            final boolean enableCompression,
            final String queryString,
            final Map<SettingKey, Serializable> settings
    ) {
        this.stage = stage;
        this.queryId = queryId;
        this.settings = settings;
        this.clientContext = clientContext;
        this.enableCompression = enableCompression;
        this.queryString = queryString;
    }

    @Override
    public ProtoType type() {
        return ProtoType.REQUEST_QUERY;
    }

    @Override
    public void writeImpl(final BinarySerializer serializer) throws IOException, SQLException {
        serializer.writeUTF8StringBinary(queryId);
        clientContext.writeTo(serializer);

        for (final Map.Entry<SettingKey, Serializable> entry : settings.entrySet()) {
            serializer.writeUTF8StringBinary(entry.getKey().name());
            SettingType type = entry.getKey().type();
            //noinspection unchecked
            type.serializeSetting(serializer, entry.getValue());
        }
        serializer.writeUTF8StringBinary("");
        serializer.writeVarInt(stage);
        serializer.writeBoolean(enableCompression);
        serializer.writeUTF8StringBinary(queryString);
        // empty data to server
        DataRequest.EMPTY.writeTo(serializer);
    }
}
