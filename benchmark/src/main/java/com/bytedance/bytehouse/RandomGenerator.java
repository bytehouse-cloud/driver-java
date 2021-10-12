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
package com.bytedance.bytehouse;

import com.bytedance.bytehouse.data.type.DataTypeInt32;
import com.bytedance.bytehouse.jdbc.ByteHouseArray;
import com.bytedance.bytehouse.jdbc.ByteHouseStruct;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.UUID;

@State(Scope.Thread)
public class RandomGenerator {
    public Object getData(Type type) {
        switch (type) {
            case Int8:
            case Int16:
            case Int32:
            case Int64:
            case UInt8:
            case UInt16:
            case UInt32:
            case UInt64:
                return 1;
            case String:
                return "HelloWorldHelloWorld";
            case FixedString:
                return "HelloWorld";
            case UUID:
                return UUID.fromString("bef35f40-3b03-45b0-b1bd-8ec6593dcaaa");
            case Float32:
                return Float.MAX_VALUE;
            case Float64:
                return Double.MAX_VALUE;
            case Decimal:
                return new BigDecimal(123);
            case Date:
                return new Date(System.currentTimeMillis());
            case DateTime:
                return new Timestamp(1530374400);
            case IPv4:
                return (long) (1L << 32) - 1;
            case IPv6:
                return "2001:44c8:129:2632:33:0:252:2";
            case Array:
                return new ByteHouseArray(new DataTypeInt32(), new Integer[]{-2147483648, 2147483647});
            case Map:
                return new HashMap<Integer, Integer>() {{
                    put(1, 1);
                }};
            case Tuple:
                return new ByteHouseStruct("Tuple", new Object[]{"test_string", 1});
            default:
                return -1;
        }
    }
}
