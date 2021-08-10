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
package com.bytedance.bytehouse.data.type.complex;

import com.bytedance.bytehouse.data.DataTypeFactory;
import com.bytedance.bytehouse.data.IDataType;
import com.bytedance.bytehouse.jdbc.ByteHouseStruct;
import com.bytedance.bytehouse.misc.SQLLexer;
import com.bytedance.bytehouse.misc.Validate;
import com.bytedance.bytehouse.serde.BinaryDeserializer;
import com.bytedance.bytehouse.serde.BinarySerializer;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.Struct;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

public class DataTypeTuple implements IDataType<ByteHouseStruct, Struct> {

    private final String name;

    public static DataTypeCreator<ByteHouseStruct, Struct> creator = (lexer, serverContext) -> {
        Validate.isTrue(lexer.character() == '(');
        List<IDataType<?, ?>> nestedDataTypes = new ArrayList<>();

        for (; ; ) {
            nestedDataTypes.add(DataTypeFactory.get(lexer, serverContext));
            char delimiter = lexer.character();
            Validate.isTrue(delimiter == ',' || delimiter == ')');
            if (delimiter == ')') {
                StringBuilder builder = new StringBuilder("Tuple(");
                for (int i = 0; i < nestedDataTypes.size(); i++) {
                    if (i > 0)
                        builder.append(",");
                    builder.append(nestedDataTypes.get(i).name());
                }
                return new DataTypeTuple(builder.append(")").toString(), nestedDataTypes.toArray(new IDataType[0]));
            }
        }
    };

    private final IDataType<?, ?>[] nestedTypes;

    public DataTypeTuple(String name, IDataType<?, ?>[] nestedTypes) {
        this.name = name;
        this.nestedTypes = nestedTypes;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public int sqlTypeId() {
        return Types.STRUCT;
    }

    @Override
    public ByteHouseStruct defaultValue() {
        Object[] attrs = new Object[getNestedTypes().length];
        for (int i = 0; i < getNestedTypes().length; i++) {
            attrs[i] = getNestedTypes()[i].defaultValue();
        }
        return new ByteHouseStruct("Tuple", attrs);
    }

    @Override
    public Class<ByteHouseStruct> javaType() {
        return ByteHouseStruct.class;
    }

    @Override
    public Class<Struct> jdbcJavaType() {
        return Struct.class;
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
    public void serializeBinary(ByteHouseStruct data, BinarySerializer serializer) throws SQLException, IOException {
        for (int i = 0; i < getNestedTypes().length; i++) {
            getNestedTypes()[i].serializeBinary(data.getAttributes()[i], serializer);
        }
    }

    @Override
    public void serializeBinaryBulk(ByteHouseStruct[] data, BinarySerializer serializer) throws SQLException, IOException {
        for (int i = 0; i < getNestedTypes().length; i++) {
            Object[] elemsData = new Object[data.length];
            for (int row = 0; row < data.length; row++) {
                elemsData[row] = ((Struct) data[row]).getAttributes()[i];
            }
            getNestedTypes()[i].serializeBinaryBulk(elemsData, serializer);
        }
    }

    @Override
    public ByteHouseStruct deserializeBinary(BinaryDeserializer deserializer) throws SQLException, IOException {
        Object[] attrs = new Object[getNestedTypes().length];
        for (int i = 0; i < getNestedTypes().length; i++) {
            attrs[i] = getNestedTypes()[i].deserializeBinary(deserializer);
        }
        return new ByteHouseStruct("Tuple", attrs);
    }

    @Override
    public ByteHouseStruct[] deserializeBinaryBulk(int rows, BinaryDeserializer deserializer) throws SQLException, IOException {
        Object[][] rowsWithElems = getRowsWithElems(rows, deserializer);

        ByteHouseStruct[] rowsData = new ByteHouseStruct[rows];
        for (int row = 0; row < rows; row++) {
            Object[] elemsData = new Object[getNestedTypes().length];

            for (int elemIndex = 0; elemIndex < getNestedTypes().length; elemIndex++) {
                elemsData[elemIndex] = rowsWithElems[elemIndex][row];
            }
            rowsData[row] = new ByteHouseStruct("Tuple", elemsData);
        }
        return rowsData;
    }

    private Object[][] getRowsWithElems(int rows, BinaryDeserializer deserializer) throws IOException, SQLException {
        Object[][] rowsWithElems = new Object[getNestedTypes().length][];
        for (int index = 0; index < getNestedTypes().length; index++) {
            rowsWithElems[index] = getNestedTypes()[index].deserializeBinaryBulk(rows, deserializer);
        }
        return rowsWithElems;
    }

    @Override
    public ByteHouseStruct deserializeText(SQLLexer lexer) throws SQLException {
        Validate.isTrue(lexer.character() == '(');
        Object[] tupleData = new Object[getNestedTypes().length];
        for (int i = 0; i < getNestedTypes().length; i++) {
            if (i > 0)
                Validate.isTrue(lexer.character() == ',');
            tupleData[i] = getNestedTypes()[i].deserializeText(lexer);
        }
        Validate.isTrue(lexer.character() == ')');
        return new ByteHouseStruct("Tuple", tupleData);
    }

    public IDataType[] getNestedTypes() {
        return nestedTypes;
    }
}
