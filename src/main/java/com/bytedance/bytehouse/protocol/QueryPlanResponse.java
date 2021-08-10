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
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class QueryPlanResponse implements Response {

    private final List<String> plans;

    public QueryPlanResponse(List<String> plans) {
        this.plans = plans;
    }

    /**
     * readFrom implementation follows
     * <a href="https://code.byted.org/bytehouse/driver-go/blob/main/driver/response/query_plan.go">
     * query_plan.go
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

    @Override
    public ProtoType type() {
        return ProtoType.RESPONSE_QUERY_PLAN;
    }

    public List<String> plans() {
        return new ArrayList<>(this.plans);
    }
}
