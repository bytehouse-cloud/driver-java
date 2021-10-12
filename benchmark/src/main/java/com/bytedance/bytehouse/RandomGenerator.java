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
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Random;
import java.util.UUID;

@SuppressWarnings({"PMD.FieldNamingConventions", "PMD.ConstructorCallsOverridableMethod"})
@State(Scope.Thread)
public class RandomGenerator {
    private final Short UINT8_MIN_VALUE = 0;
    private final Short UINT8_MAX_VALUE = 255;
    private final Integer UINT16_MIN_VALUE = 0;
    private final Integer UINT16_MAX_VALUE = 65535;
    private final Long UINT32_MIN_VALUE = 0L;
    private final Long UINT32_MAX_VALUE = 4294967295L;
    private final BigInteger UINT64_MIN_VALUE = BigInteger.valueOf(0L);
    private final BigInteger UINT64_MAX_VALUE = new BigInteger("18446744073709551615");

    private final Byte INT8_MIN_VALUE = -128;
    private final Byte INT8_MAX_VALUE = 127;
    private final Short INT16_MIN_VALUE = -32768;
    private final Short INT16_MAX_VALUE = 32767;
    private final Integer INT32_MIN_VALUE = -2147483648;
    private final Integer INT32_MAX_VALUE = 2147483647;
    private final Long INT64_MIN_VALUE = -9223372036854775808L;
    private final Long INT64_MAX_VALUE = 9223372036854775807L;

    private final int SIZE = 1000000;
    private Short[] UINT8s = new Short[SIZE];
    private Integer[] UINT16s = new Integer[SIZE];
    private Long[] UINT32s = new Long[SIZE];
    private BigInteger[] UINT64s = new BigInteger[SIZE];
    private Byte[] INT8s = new Byte[SIZE];
    private Short[] INT16s = new Short[SIZE];
    private Integer[] INT32s = new Integer[SIZE];
    private Long[] INT64s = new Long[SIZE];
    private String[] STRINGs = new String[SIZE];
    private String[] FIXEDSTRINGs = new String[SIZE];
    private Float[] FLOATs = new Float[SIZE];
    private Double[] DOUBLEs = new Double[SIZE];
    private UUID[] UUIDs = new UUID[SIZE];

    private int UINT8ptr = 0;
    private int UINT16ptr = 0;
    private int UINT32ptr = 0;
    private int UINT64ptr = 0;
    private int INT8ptr = 0;
    private int INT16ptr = 0;
    private int INT32ptr = 0;
    private int INT64ptr = 0;
    private int STRINGptr = 0;
    private int FIXEDSTRINGptr = 0;
    private int FLOATptr = 0;
    private int DOUBLEptr = 0;
    private int UUIDptr = 0;

    private Random random;
    private String alphaNumericString = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvxyz";

    private final int LEN = alphaNumericString.length();

    public RandomGenerator() {
        random = new Random();
        for (int i=0; i<SIZE; i++) {
            UINT8s[i] = (short) generateInt(UINT8_MIN_VALUE, UINT8_MAX_VALUE);
            UINT16s[i] = generateInt(UINT16_MIN_VALUE, UINT16_MAX_VALUE);
            UINT32s[i] = generateLong(UINT32_MIN_VALUE, UINT32_MAX_VALUE);
            UINT64s[i] = BigInteger.valueOf(generateLong(INT64_MIN_VALUE, INT64_MAX_VALUE));
            INT8s[i] = (byte) generateInt(INT8_MIN_VALUE, INT8_MAX_VALUE);
            INT16s[i] = (short) generateInt(INT16_MIN_VALUE, INT16_MAX_VALUE);
            INT32s[i] = generateInt(INT32_MIN_VALUE, INT32_MAX_VALUE);
            INT64s[i] = generateLong(INT64_MIN_VALUE, INT64_MAX_VALUE);
            STRINGs[i] = generateString(20);
            FIXEDSTRINGs[i] = generateString(10);
            FLOATs[i] = generateFloat();
            DOUBLEs[i] = generateDouble();
            UUIDs[i] = UUID.randomUUID();
        }
    }

    public int generateInt(double min, double max) {
        return (int) ((Math.random() * (max - min)) + min);
    }

    public long generateLong(double min, double max) {
        return (long) ((Math.random() * (max - min)) + min);
    }

    public String generateString(int len) {
        StringBuilder builder = new StringBuilder();
        for (int i=0; i<len; i++) {
            int index = generateInt(0, LEN);
            builder.append(alphaNumericString.charAt(index));
        }
        return builder.toString();
    }

    public Float generateFloat() {
        return random.nextFloat();
    }

    public Double generateDouble() {
        return Math.random();
    }

    public Integer[] getArray(int len) {
        Integer[] integers = new Integer[len];
        for (int i=0; i<len; i++) {
            if (INT32ptr == SIZE) INT32ptr = 0;
            integers[i] = INT32s[INT32ptr++];
        }
        return integers;
    }

    public HashMap<Integer, Integer> getHashMap() {
        return new HashMap<Integer, Integer>() {{
            if (INT32ptr == SIZE) INT32ptr = 0;
            int key = INT32s[INT32ptr++];
            if (INT32ptr == SIZE) INT32ptr = 0;
            int value = INT32s[INT32ptr++];
            put(key, value);
        }};
    }

    public Object getData(Type type) {
        switch (type) {
            case Int8:
                if (INT8ptr == SIZE) INT8ptr = 0;
                return INT8s[INT8ptr++];
            case Int16:
                if (INT16ptr == SIZE) INT16ptr = 0;
                return INT16s[INT16ptr++];
            case Int32:
                if (INT32ptr == SIZE) INT32ptr = 0;
                return INT32s[INT32ptr++];
            case Int64:
                if (INT64ptr == SIZE) INT64ptr = 0;
                return INT64s[INT64ptr++];
            case UInt8:
                if (UINT8ptr == SIZE) UINT8ptr = 0;
                return UINT8s[UINT8ptr++];
            case UInt16:
                if (UINT16ptr == SIZE) UINT16ptr = 0;
                return UINT16s[UINT16ptr++];
            case UInt32:
                if (UINT32ptr == SIZE) UINT32ptr = 0;
                return UINT32s[UINT32ptr++];
            case UInt64:
                if (UINT64ptr == SIZE) UINT64ptr = 0;
                return UINT64s[UINT64ptr++];
            case String:
                if (STRINGptr == SIZE) STRINGptr = 0;
                return STRINGs[STRINGptr++];
            case FixedString:
                if (FIXEDSTRINGptr == SIZE) FIXEDSTRINGptr = 0;
                return FIXEDSTRINGs[FIXEDSTRINGptr++];
            case UUID:
                if (UUIDptr == SIZE) UUIDptr = 0;
                return UUIDs[UUIDptr++];
            case Float32:
                if (FLOATptr == SIZE) FLOATptr = 0;
                return FLOATs[FLOATptr++];
            case Float64:
                if (DOUBLEptr == SIZE) DOUBLEptr = 0;
                return DOUBLEs[DOUBLEptr++];
            case Decimal:
                return new BigDecimal(123);
            case Date:
                if (INT64ptr == SIZE) INT64ptr = 0;
                return new Date(INT64s[INT64ptr++]);
            case DateTime:
                if (INT64ptr == SIZE) INT64ptr = 0;
                return new Timestamp(INT64s[INT64ptr++]);
            case IPv4:
                if (UINT32ptr == SIZE) UINT32ptr = 0;
                return UINT32s[UINT32ptr++];
            case IPv6:
                return "2001:44c8:129:2632:33:0:252:2";
            case Array:
                return new ByteHouseArray(new DataTypeInt32(), getArray(5));
            case Map:
                return getHashMap();
            case Tuple:
                return new ByteHouseStruct("Tuple", new Object[]{"test_string", 1});
            default:
                return -1;
        }
    }
}
