package com.bytedance.bytehouse.protocol;

import com.bytedance.bytehouse.serde.BinaryDeserializer;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class AggQueryPlanResponse implements Response {

    /**
     * readFrom implementation follows
     * <a href="https://code.byted.org/bytehouse/driver-go/blob/main/driver/response/aggregate_query_plan.go">
     *     aggregate_query_plan.go
     * </a>
     */
    public static AggQueryPlanResponse readFrom(
            BinaryDeserializer deserializer) throws IOException, SQLException {
        List<String> plans = new ArrayList<>();
        long count = deserializer.readVarInt();

        for (int i = 0; i < count; i++) {
            plans.add(deserializer.readUTF8StringBinary());
        }

        return new AggQueryPlanResponse(plans);
    }

    private final List<String> plans;

    public AggQueryPlanResponse(List<String> plans) {
        this.plans = plans;
    }

    @Override
    public ProtoType type() {
        return ProtoType.RESPONSE_AGG_QUERY_PLAN;
    }

    public List<String> plans() {
        return new ArrayList<>(this.plans);
    }
}