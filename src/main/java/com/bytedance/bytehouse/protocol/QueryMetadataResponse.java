package com.bytedance.bytehouse.protocol;

import com.bytedance.bytehouse.serde.BinaryDeserializer;
import java.io.IOException;

public class QueryMetadataResponse implements Response {

    /**
     * readFrom implementation follows
     * <a href="https://code.byted.org/bytehouse/driver-go/blob/main/driver/response/query_metadata.go">
     *     query_metadata.go
     * </a>
     */
    public static QueryMetadataResponse readFrom(BinaryDeserializer deserializer) throws IOException {
        return new QueryMetadataResponse(deserializer.readUTF8StringBinary());
    }

    private final String queryId;

    public QueryMetadataResponse(String queryId) {
        this.queryId = queryId;
    }

    @Override
    public ProtoType type() {
        return ProtoType.RESPONSE_QUERY_METADATA;
    }

    public String queryId() {
        return queryId;
    }
}
