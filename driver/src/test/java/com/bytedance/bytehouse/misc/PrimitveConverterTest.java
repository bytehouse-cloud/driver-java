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
package com.bytedance.bytehouse.misc;

import org.junit.jupiter.api.Test;

import static com.bytedance.bytehouse.misc.PrimitiveConverter.box;
import static org.junit.jupiter.api.Assertions.assertEquals;

class PrimitiveConverterTest {
    private void assertArrayEquals(Object[] expected, Object[] actual) {
        assertEquals(expected.length, actual.length);
        for (int i=0; i<expected.length; i++) {
            assertEquals(expected[i], actual[i]);
        }
    }

    @Test
    public void testInt() {
        int[] intArrays = new int[]{1, 2};
        Integer[] IntegerArrays = new Integer[]{1, 2};
        assertArrayEquals(IntegerArrays, box(intArrays));
    }

    @Test
    public void testEmptyArray() {
        int[] intArrays = new int[]{};
        Integer[] IntegerArrays = new Integer[]{};
        assertArrayEquals(IntegerArrays, box(intArrays));
    }

    @Test
    public void testNoConversion() {
        Integer[] IntegerArrays = new Integer[]{1, 2};
        assertArrayEquals(IntegerArrays, box(IntegerArrays));
    }
}
