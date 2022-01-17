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

import static java.lang.reflect.Array.*;

import java.lang.reflect.Array;

public class PrimitiveConverter {

    public static Object[] box(Object src) {
        int length = Array.getLength(src);
        Object[] dest = (Object[]) newInstance(typeCastTo(src.getClass().getComponentType()), length);
        for (int i=0; i<length; i++) {
            set(dest, i, get(src, i));
        }
        return dest;
    }

    private static Class<?> typeCastTo(Class<?> type) {
        if (type.equals(boolean.class)) return Boolean.class;
        if (type.equals(byte.class)) return Byte.class;
        if (type.equals(char.class)) return Character.class;
        if (type.equals(double.class)) return Double.class;
        if (type.equals(float.class)) return Float.class;
        if (type.equals(int.class)) return Integer.class;
        if (type.equals(long.class)) return Long.class;
        if (type.equals(short.class)) return Short.class;
        if (type.equals(void.class)) return Void.class;
        return type;
    }
}
