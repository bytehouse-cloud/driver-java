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
import com.bytedance.bytehouse.jdbc.wrapper.SQLStruct;
import com.bytedance.bytehouse.log.Logger;
import com.bytedance.bytehouse.log.LoggerFactoryUtils;
import com.bytedance.bytehouse.misc.ValidateUtils;
import java.sql.SQLException;
import java.util.Map;
import java.util.StringJoiner;
import java.util.function.BiFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ByteHouseStruct implements SQLStruct {

    private static final Logger LOG = LoggerFactoryUtils.getLogger(ByteHouseStruct.class);

    private static final Pattern ATTR_INDEX_REGEX = Pattern.compile("_(\\d+)");

    private final String type;

    private final Object[] attributes;

    public ByteHouseStruct(String type, Object[] attributes) {
        this.type = type;
        this.attributes = attributes;
    }

    @Override
    public String getSQLTypeName() throws SQLException {
        return type;
    }

    @Override
    public Object[] getAttributes() throws SQLException {
        return attributes;
    }

    @Override
    public Object[] getAttributes(Map<String, Class<?>> map) throws SQLException {
        int i = 0;
        Object[] res = new Object[map.size()];
        for (String attrName : map.keySet()) {
            Class<?> clazz = map.get(attrName);
            Matcher matcher = ATTR_INDEX_REGEX.matcher(attrName);
            ValidateUtils.isTrue(matcher.matches(), "Can't find " + attrName + ".");

            int attrIndex = Integer.parseInt(matcher.group(1)) - 1;
            ValidateUtils.isTrue(attrIndex < attributes.length, "Can't find " + attrName + ".");
            ValidateUtils.isTrue(clazz.isInstance(attributes[attrIndex]),
                    "Can't cast " + attrName + " to " + clazz.getName());

            res[i++] = clazz.cast(attributes[attrIndex]);
        }
        return res;
    }

    @Override
    public Logger logger() {
        return ByteHouseStruct.LOG;
    }

    @Override
    public String toString() {
        StringJoiner joiner = new StringJoiner(",", "(", ")");
        for (Object item : attributes) {
            // TODO format by itemDataType
            joiner.add(String.valueOf(item));
        }
        return joiner.toString();
    }

    // actually we should hold nestedTypes on this
    public ByteHouseStruct mapAttributes(IDataType<?, ?>[] nestedTypes, BiFunction<IDataType<?, ?>, Object, Object> mapFunc) {
        assert nestedTypes.length == attributes.length;
        Object[] mapped = new Object[attributes.length];
        for (int i = 0; i < attributes.length; i++) {
            mapped[i] = mapFunc.apply(nestedTypes[i], attributes[i]);
        }
        return new ByteHouseStruct(type, mapped);
    }
}
