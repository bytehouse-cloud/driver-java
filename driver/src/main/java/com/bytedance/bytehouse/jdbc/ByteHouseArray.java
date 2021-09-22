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
package com.bytedance.bytehouse.jdbc;

import com.bytedance.bytehouse.data.IDataType;
import com.bytedance.bytehouse.jdbc.wrapper.SQLArray;
import com.bytedance.bytehouse.log.Logger;
import com.bytedance.bytehouse.log.LoggerFactory;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.StringJoiner;
import java.util.function.BiFunction;

public class ByteHouseArray implements SQLArray {

    private static final Logger LOG = LoggerFactory.getLogger(ByteHouseArray.class);

    private final IDataType<?, ?> elementDataType;

    private final Object[] elements;

    public ByteHouseArray(IDataType<?, ?> elementDataType, Object[] elements) {
        this.elementDataType = elementDataType;
        this.elements = elements;
    }

    @Override
    public String getBaseTypeName() throws SQLException {
        return elementDataType.name();
    }

    @Override
    public int getBaseType() {
        return elementDataType.sqlTypeId();
    }

    @Override
    public void free() throws SQLException {
    }

    @Override
    public Object[] getArray() throws SQLException {
        return elements;
    }

    @Override
    public Logger logger() {
        return ByteHouseArray.LOG;
    }

    @Override
    public String toString() {
        StringJoiner joiner = new StringJoiner(",", "[", "]");
        for (Object item : elements) {
            // TODO format by itemDataType
            joiner.add(String.valueOf(item));
        }
        return joiner.toString();
    }

    public ByteHouseArray slice(int offset, int length) {
        Object[] result = new Object[length];
        if (length >= 0) System.arraycopy(elements, offset, result, 0, length);
        return new ByteHouseArray(elementDataType, result);
    }

    public ByteHouseArray mapElements(BiFunction<IDataType<?, ?>, Object, Object> mapFunc) {
        Object[] mapped = Arrays.stream(elements).map(elem -> mapFunc.apply(elementDataType, elem)).toArray();
        return new ByteHouseArray(elementDataType, mapped);
    }
}
