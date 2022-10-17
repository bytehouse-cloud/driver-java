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
package com.bytedance.bytehouse.data.type.complex;

import com.bytedance.bytehouse.data.DataTypeFactory;
import com.bytedance.bytehouse.data.IDataType;
import com.bytedance.bytehouse.data.type.DataTypeInt64;
import com.bytedance.bytehouse.jdbc.ByteHouseArray;
import com.bytedance.bytehouse.misc.SQLLexer;
import com.bytedance.bytehouse.misc.ValidateUtils;
import com.bytedance.bytehouse.serde.BinaryDeserializer;
import com.bytedance.bytehouse.serde.BinarySerializer;
import java.io.IOException;
import java.sql.Array;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

// TODO avoid using ByteHouseArray because it's a subclass of java.sql.Array
public class DataTypeArray implements IDataType<ByteHouseArray, Array> {

    private final String name;

    public static DataTypeCreator<ByteHouseArray, Array> creator = (lexer, serverContext) -> {
        ValidateUtils.isTrue(lexer.character() == '(');
        IDataType<?, ?> arrayNestedType = DataTypeFactory.get(lexer, serverContext);
        ValidateUtils.isTrue(lexer.character() == ')');
        return new DataTypeArray("Array(" + arrayNestedType.name() + ")",
                arrayNestedType, (DataTypeInt64) DataTypeFactory.get("Int64", serverContext));
    };

    private final ByteHouseArray defaultValue;

    private final IDataType<?, ?> elemDataType;

    // Change from UInt64 to Int64 because we mapping UInt64 to BigInteger
    private final DataTypeInt64 offsetIDataType;

    public DataTypeArray(String name, IDataType<?, ?> elemDataType, DataTypeInt64 offsetIDataType) throws SQLException {
        this.name = name;
        this.elemDataType = elemDataType;
        this.offsetIDataType = offsetIDataType;
        this.defaultValue = new ByteHouseArray(elemDataType, new Object[]{elemDataType.defaultValue()});
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public int sqlTypeId() {
        return Types.ARRAY;
    }

    @Override
    public ByteHouseArray defaultValue() {
        return defaultValue;
    }

    @Override
    public Class<ByteHouseArray> javaType() {
        return ByteHouseArray.class;
    }

    @Override
    public Class<Array> jdbcJavaType() {
        return Array.class;
    }

    @Override
    public int getPrecision() {
        return 0;
    }

    @Override
    public int getScale() {
        return 0;
    }

    @Override
    public ByteHouseArray deserializeText(SQLLexer lexer) throws SQLException {
        ValidateUtils.isTrue(lexer.character() == '[');
        List<Object> arrayData = new ArrayList<>();
        for (; ; ) {
            if (lexer.isCharacter(']')) {
                lexer.character();
                break;
            }
            if (lexer.isCharacter(',')) {
                lexer.character();
            }
            arrayData.add(elemDataType.deserializeText(lexer));
        }
        return new ByteHouseArray(elemDataType, arrayData.toArray());
    }

    @Override
    public void serializeBinary(ByteHouseArray data, BinarySerializer serializer) throws SQLException, IOException {
        for (Object f : data.getArray()) {
            getElemDataType().serializeBinary(f, serializer);
        }
    }

    @Override
    public void serializeBinaryBulk(ByteHouseArray[] data, BinarySerializer serializer) throws SQLException, IOException {
        offsetIDataType.serializeBinary((long) data.length, serializer);
        getElemDataType().serializeBinaryBulk(data, serializer);
    }

    @Override
    public ByteHouseArray deserializeBinary(BinaryDeserializer deserializer) throws SQLException, IOException {
        Long offset = offsetIDataType.deserializeBinary(deserializer);
        Object[] data = getElemDataType().deserializeBinaryBulk(offset.intValue(), deserializer);
        return new ByteHouseArray(elemDataType, data);
    }

    @Override
    public ByteHouseArray[] deserializeBinaryBulk(int rows, BinaryDeserializer deserializer) throws IOException, SQLException {
        ByteHouseArray[] arrays = new ByteHouseArray[rows];
        if (rows == 0) {
            return arrays;
        }

        int[] offsets = Arrays.stream(offsetIDataType.deserializeBinaryBulk(rows, deserializer)).mapToInt(value -> ((Long) value).intValue()).toArray();
        ByteHouseArray res = new ByteHouseArray(elemDataType,
                elemDataType.deserializeBinaryBulk(offsets[rows - 1], deserializer));

        for (int row = 0, lastOffset = 0; row < rows; row++) {
            int offset = offsets[row];
            arrays[row] = res.slice(elemDataType.allocate(offset - lastOffset), lastOffset, offset - lastOffset);
            lastOffset = offset;
        }
        return arrays;
    }

    public IDataType getElemDataType() {
        return elemDataType;
    }

    @Override
    public ByteHouseArray[] allocate(int rows) {
        return new ByteHouseArray[rows];
    }
}
