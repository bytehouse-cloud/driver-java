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
import com.bytedance.bytehouse.exception.NotImplementedException;
import com.bytedance.bytehouse.serde.BinaryDeserializer;
import java.io.IOException;
import java.sql.SQLException;

/**
 * {@link Response} contains all the details about different types of supported responses
 */
public interface Response {

    static Response readFrom(
            final BinaryDeserializer deserializer,
            final ServerContext info
    ) throws IOException, SQLException {
        int responseType = (int) deserializer.readVarInt();
        switch (responseType) {
            case 0:
                return HelloResponse.readFrom(deserializer);
            case 1:
                return DataResponse.readFrom(deserializer, info);
            case 2:
                throw ExceptionResponse.readExceptionFrom(deserializer);
            case 3:
                return ProgressResponse.readFrom(deserializer);
            case 4:
                return PongResponse.readFrom(deserializer);
            case 5:
                return EOFStreamResponse.readFrom(deserializer);
            case 6:
                return ProfileInfoResponse.readFrom(deserializer);
            case 7:
                return TotalsResponse.readFrom(deserializer, info);
            case 8:
                return ExtremesResponse.readFrom(deserializer, info);
            case 9:
                throw new NotImplementedException("RESPONSE_TABLES_STATUS_RESPONSE");
            case 10:
                return LogResponse.readFrom(deserializer, info);
            case 11:
                return TableColumnsResponse.readFrom(deserializer);
            case 12:
                return QueryPlanResponse.readFrom(deserializer);
            case 13:
                return AggQueryPlanResponse.readFrom(deserializer);
            case 14:
                return QueryMetadataResponse.readFrom(deserializer);
            default:
                throw new IllegalStateException(
                        String.format("Unknown server response type: %d", responseType)
                );
        }
    }

    ProtoType type();

    enum ProtoType {
        RESPONSE_HELLO(0),
        RESPONSE_DATA(1),
        RESPONSE_EXCEPTION(2),
        RESPONSE_PROGRESS(3),
        RESPONSE_PONG(4),
        RESPONSE_END_OF_STREAM(5),
        RESPONSE_PROFILE_INFO(6),
        RESPONSE_TOTALS(7),
        RESPONSE_EXTREMES(8),
        RESPONSE_TABLES_STATUS_RESPONSE(9),
        RESPONSE_LOG(10),
        RESPONSE_TABLE_COLUMNS(11),
        RESPONSE_QUERY_PLAN(12),
        RESPONSE_AGG_QUERY_PLAN(13),
        RESPONSE_QUERY_METADATA(14);

        private final int id;

        ProtoType(int id) {
            this.id = id;
        }

        public long id() {
            return id;
        }
    }
}
