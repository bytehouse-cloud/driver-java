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
package com.bytedance.bytehouse.data;

import static com.bytedance.bytehouse.misc.ExceptionUtil.unchecked;

import com.bytedance.bytehouse.data.type.DataTypeBitMap64;
import com.bytedance.bytehouse.data.type.complex.DataTypeArray;
import com.bytedance.bytehouse.data.type.complex.DataTypeNothing;
import com.bytedance.bytehouse.data.type.complex.DataTypeNullable;
import com.bytedance.bytehouse.data.type.complex.DataTypeTuple;
import com.bytedance.bytehouse.exception.ByteHouseSQLException;
import com.bytedance.bytehouse.jdbc.ByteHouseArray;
import com.bytedance.bytehouse.jdbc.ByteHouseStruct;
import com.bytedance.bytehouse.log.Logger;
import com.bytedance.bytehouse.log.LoggerFactoryUtils;
import java.time.ZoneId;

/**
 * Converter layer between internal representation of ByteHouse data type (Java type) and JDBC types.
 */
public class DataTypeConverter {

    private static final Logger LOG = LoggerFactoryUtils.getLogger(DataTypeConverter.class);

    private final ZoneId tz;

    public DataTypeConverter(ZoneId tz) {
        this.tz = tz;
    }

    /**
     * Converts JDBC type to Java type.
     */
    public Object convertJdbcToJava(IDataType<?, ?> type, Object obj) throws ByteHouseSQLException {
        if (obj == null) {
            if (type.nullable() || type instanceof DataTypeNothing)
                return null;
            throw new ByteHouseSQLException(-1, "type[" + type.name() + "] doesn't support null value");
        }
        // handle special types
        if (type instanceof DataTypeNullable) {
            // handled null at first, so obj also not null here
            return convertJdbcToJava(((DataTypeNullable) type).getNestedDataType(), obj);
        }
        if (type instanceof DataTypeArray) {
            if (!(obj instanceof ByteHouseArray)) {
                throw new ByteHouseSQLException(-1, "require ByteHouseArray for column: " + type.name() + ", but found " + obj.getClass());
            }
            return ((ByteHouseArray) obj).mapElements(unchecked(this::convertJdbcToJava));
        }
        if (type instanceof DataTypeBitMap64) {
            if (!(obj instanceof ByteHouseArray)) {
                throw new ByteHouseSQLException(-1, "require ByteHouseArray for column: " + type.name() + ", but found " + obj.getClass());
            }
            return ((ByteHouseArray) obj).mapElements(unchecked(this::convertJdbcToJava));
        }
        if (type instanceof DataTypeTuple) {
            if (!(obj instanceof ByteHouseStruct)) {
                throw new ByteHouseSQLException(-1, "require ByteHouseStruct for column: " + type.name() + ", but found " + obj.getClass());
            }
            return ((ByteHouseStruct) obj).mapAttributes(((DataTypeTuple) type).getNestedTypes(), unchecked(this::convertJdbcToJava));
        }
        // convert jdbc to java type
        return type.convertJdbcToJavaType(obj, tz);
    }
}
