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

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CollectionUtilTest {

    private static final Map<String, String> MAP_1 = new HashMap<>();

    static {
        MAP_1.put("k1", "v1");
        MAP_1.put("k2", "v2");
    }

    private static final Map<String, String> MAP_2 = new HashMap<>();

    static {
        MAP_2.put("k2", "new_v2");
        MAP_2.put("k3", "v3");
    }

    private static final Map<String, String> KEEP_FIRST_MAP = new HashMap<>();

    static {
        KEEP_FIRST_MAP.put("k1", "v1");
        KEEP_FIRST_MAP.put("k2", "v2");
        KEEP_FIRST_MAP.put("k3", "v3");
    }

    private static final Map<String, String> KEEP_LAST_MAP = new HashMap<>();

    static {
        KEEP_LAST_MAP.put("k1", "v1");
        KEEP_LAST_MAP.put("k2", "new_v2");
        KEEP_LAST_MAP.put("k3", "v3");
    }

    @Test
    public void testMergeMap() {
        assertEquals(KEEP_FIRST_MAP, CollectionUtil.mergeMapKeepFirst(MAP_1, MAP_2));
        assertEquals(KEEP_LAST_MAP, CollectionUtil.mergeMapKeepLast(MAP_1, MAP_2));
    }

    @Test
    public void testMergeMapInPlace() {
        HashMap<String, String> map1Copy1 = new HashMap<>(MAP_1);
        CollectionUtil.mergeMapInPlaceKeepFirst(map1Copy1, MAP_2);
        assertEquals(KEEP_FIRST_MAP, map1Copy1);
        HashMap<String, String> map1Copy2 = new HashMap<>(MAP_1);
        CollectionUtil.mergeMapInPlaceKeepLast(map1Copy2, MAP_2);
        assertEquals(KEEP_LAST_MAP, map1Copy2);
    }
}
