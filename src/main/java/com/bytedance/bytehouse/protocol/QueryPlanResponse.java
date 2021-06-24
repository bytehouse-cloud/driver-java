package com.bytedance.bytehouse.protocol;

import com.bytedance.bytehouse.serde.BinaryDeserializer;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class QueryPlanResponse implements Response {

    /**
     * readFrom implementation follows
     * <a href="https://code.byted.org/bytehouse/driver-go/blob/main/driver/response/query_plan.go">
     *     query_plan.go
     * </a>
     */
    public static QueryPlanResponse readFrom(
            BinaryDeserializer deserializer) throws IOException, SQLException {
        List<String> plans = new ArrayList<>();
        long count = deserializer.readVarInt();

        for (int i = 0; i < count; i++) {
            plans.add(deserializer.readUTF8StringBinary());
        }

        return new QueryPlanResponse(plans);
    }

    private final List<String> plans;

    public QueryPlanResponse(List<String> plans) {
        this.plans = plans;
    }

    @Override
    public ProtoType type() {
        return ProtoType.RESPONSE_QUERY_PLAN;
    }

    public List<String> plans() {
        return new ArrayList<>(this.plans);
    }
}
