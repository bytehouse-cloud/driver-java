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
import com.bytedance.bytehouse.exception.ByteHouseSQLException;
import com.bytedance.bytehouse.misc.BytesHelper;
import com.bytedance.bytehouse.misc.SQLLexer;
import com.bytedance.bytehouse.misc.Validate;
import com.bytedance.bytehouse.serde.BinaryDeserializer;
import com.bytedance.bytehouse.serde.BinarySerializer;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.Types;
import java.time.ZoneId;

public class DataTypeLowCardinality implements IDataType<Object, Object>, BytesHelper {

    private final IDataType<?, ?> keys;

    public static DataTypeCreator<Object, Object> creator = (lexer, serverContext) -> {
        Validate.isTrue(lexer.character() == '(');
        IDataType<?, ?> elemDataType = DataTypeFactory.get(lexer, serverContext);
        Validate.isTrue(lexer.character() == ')');

        return new DataTypeLowCardinality(elemDataType);
    };

    private byte[] header;

    private byte[] valueIndicesRaw;

    public DataTypeLowCardinality(IDataType<?, ?> elemDataType) {
        this.keys = elemDataType;
        this.header = new byte[24];
    }

    @Override
    public String name() {
        return String.format("LowCardinality(%s)", keys.name());
    }

    @Override
    public Class<Object> javaType() {
        return Object.class;
    }

    @Override
    public int sqlTypeId() {
        return Types.OTHER;
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
    public void serializeBinary(Object data, BinarySerializer serializer) throws SQLException, IOException {
        throw new UnsupportedOperationException("You should not serialize a single LowCardinality value. " +
                "It is always done in bulk.");
    }

    @Override
    public void serializeBinaryBulk(Object[] data, BinarySerializer serializer) throws SQLException, IOException {
        throw new UnsupportedOperationException("This method should not be called. Bulk serialization of " +
                "Low Cardinality type is performed by ColumnLowCardinality.");
    }

    @Override
    public Object deserializeBinary(BinaryDeserializer deserializer) throws SQLException, IOException {
        throw new UnsupportedOperationException("You should not deserialize a single Low Cardinality value. " +
                "It is always done in bulk.");
    }

    @Override
    public Object deserializeText(SQLLexer lexer) throws SQLException {
        Object res = keys.deserializeText(lexer);
        return res;
    }

    @Override
    public Object[] deserializeBinaryBulk(int rows, BinaryDeserializer deserializer) throws SQLException {
        try {
            if (rows == 0) {
                return new Object[0];
            }

            header = deserializer.readBytes(24);

            final int numOfUniqueValues = (int) getLongLE(header, 16);
            final Object[] uniqueValues = keys.deserializeBinaryBulk(numOfUniqueValues, deserializer);
            final long numOfRows = deserializer.readLong();
            final int indexByteSize = header[8] + 1;
            valueIndicesRaw = deserializer.readBytes((int) (numOfRows * indexByteSize));
            final Object[] res = new Object[rows];

            for (int i = 0; i < rows; i++) {
                res[i] = uniqueValues[getIndex(indexByteSize, i)];
            }
            return res;
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    private int getIndex(int indexByteSize, int row) {
        switch (indexByteSize) {
            case 1:
                return valueIndicesRaw[row];
            case 2:
                return getShortLE(valueIndicesRaw, 2 * row);
            case 3:
                return getIntLE(valueIndicesRaw, 4 * row);
            case 4:
                return (int) getLongLE(valueIndicesRaw, 8 * row);
            default:
                throw new IllegalStateException("supposed unreachable execution path");
        }
    }

    public IDataType getElemDataType() {
        return keys;
    }

    @Override
    public Object convertJdbcToJavaType(Object obj, ZoneId tz) throws ByteHouseSQLException {
        return getElemDataType().convertJdbcToJavaType(obj, tz);
    }
}
