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
package com.bytedance.bytehouse.misc;

import com.bytedance.bytehouse.data.type.DataTypeUInt64;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class BytesHelperTest {

    @Test
    void getByteAsInt() {
        // positive integer 5: 0b00000101
        // negative integer -5: 0b11111011; which is 251 when interpreted unsigned
        byte[] given = {0b00000101, (byte) 0b11111011};

        int got;
        DataTypeUInt64 dtu = new DataTypeUInt64();

        got = dtu.getByteAsInt(given, 0);
        assertEquals(got, 5);

        got = dtu.getByteAsInt(given, 1);
        assertEquals(got, 251);
    }

    @Test
    void getShortAsIntLE() {
        // 5: 0b00000101 0b00000000 in LE
        // -5: 0b11111011 0b11111111 in LE; which is 65,531 when interpreted unsigned
        byte[] given = {0b00000101, 0b00000000, (byte) 0b11111011, (byte) 0b11111111};

        int got;
        DataTypeUInt64 dtu = new DataTypeUInt64();

        got = dtu.getShortAsIntLE(given, 0);
        assertEquals(got, 5);

        got = dtu.getShortAsIntLE(given, 2);
        assertEquals(got, 65531);
    }
}
