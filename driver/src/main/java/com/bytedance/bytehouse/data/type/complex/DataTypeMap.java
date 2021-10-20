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
import com.bytedance.bytehouse.data.type.DataTypeUInt64;
import com.bytedance.bytehouse.exception.ByteHouseSQLException;
import com.bytedance.bytehouse.misc.SQLLexer;
import com.bytedance.bytehouse.misc.ValidateUtils;
import com.bytedance.bytehouse.serde.BinaryDeserializer;
import com.bytedance.bytehouse.serde.BinarySerializer;
import java.io.IOException;
import java.math.BigInteger;
import java.sql.SQLException;
import java.sql.Types;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class DataTypeMap implements IDataType<Map, Object> {

    private static final DataTypeUInt64 DATA_TYPE_UINT_64 = new DataTypeUInt64();

    private final IDataType<?, ?> keyDataType;

    public static DataTypeCreator<Map, Object> creator = (lexer, serverContext) -> {
        ValidateUtils.isTrue(lexer.character() == '(');
        IDataType<?, ?> keyDataType = DataTypeFactory.get(lexer, serverContext);
        ValidateUtils.isTrue(lexer.character() == ',');
        IDataType<?, ?> valueDataType = DataTypeFactory.get(lexer, serverContext);
        ValidateUtils.isTrue(lexer.character() == ')');

        return new DataTypeMap(keyDataType, valueDataType);
    };

    private final IDataType<?, ?> valueDataType;

    public DataTypeMap(
            IDataType<?, ?> keyDataType,
            IDataType<?, ?> valueDataType
    ) throws SQLException {
        this.keyDataType = keyDataType;
        this.valueDataType = valueDataType;
    }

    @Override
    public String name() {
        return String.format("Map(%s,%s)", keyDataType.name(), valueDataType.name());
    }

    @Override
    public Class<Map> javaType() {
        return Map.class;
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

    /**
     * Map data cannot be serialized for a single row. The data for the entire column is packed together.
     */
    @Override
    public void serializeBinary(Map data, BinarySerializer serializer) throws SQLException, IOException {
        throw new UnsupportedOperationException(
                "You should not serialize a single Map value. It is always done in bulk."
        );
    }

    /**
     * Serialization of Map column is done by ColumnMap, which has the correct offsets.
     */
    @Override
    public void serializeBinaryBulk(Map[] data, BinarySerializer serializer) throws SQLException, IOException {
        throw new UnsupportedOperationException(
                "This method should not be called. Bulk serialization of Map type is performed by ColumnMap."
        );
    }

    /**
     * Map data cannot be deserialized for a single row. The data for the entire column is packed together.
     */
    @Override
    public Map deserializeBinary(BinaryDeserializer deserializer) throws SQLException, IOException {
        throw new UnsupportedOperationException(
                "You should not deserialize a single Map value. It is always done in bulk."
        );
    }

    /**
     * Deserializes Map data for the <b>*entire column*</b>(columnar format).
     *
     * <br><br>
     * The map keys for all the rows are packed into a single array (keys)<br>
     * The map values for all the rows are packed into a single array (values) as well. <br>
     * An offset array will stores which elements belong to which row <br>
     *
     * <br>
     * example
     * <p>
     * Deserialized format
     * ------------------------------------
     * | row id |  colA      |  colB      |
     * ------------------------------------
     * |   1    | {1:1,2:2} | {'a':'b'}   |
     * ------------------------------------
     * |   2    | {3:4,5:6} | {'c':'d'}   |
     * ------------------------------------
     * <p>
     * Serialized format:
     * <p>
     * key colA:    [1,2,3,5]
     * val colA:    [1,2,4,6]
     * offset colA: [2,4]  because row 1 is [0-2), row 2 is [2,4)
     * <p>
     * key colB:    ['a','c]
     * val colB:    ['b','d']
     * offset colA: [1,2]
     */
    @Override
    public Map[] deserializeBinaryBulk(int rows, BinaryDeserializer deserializer) throws IOException, SQLException {
        Map[] maps = new Map[rows];
        if (rows == 0) {
            return maps;
        }

        // keys[offsets[i - 1] : offsets[i]] gives the keys for row i (zero-indexed)
        // values[offsets[i - 1] : offsets[i]] gives the values for row i (zero-indexed)
        int[] offsets = Arrays.stream(
                DATA_TYPE_UINT_64.deserializeBinaryBulk(rows, deserializer)
        ).mapToInt(value -> ((BigInteger) value).intValue()).toArray();

        // deserialize all keys and values for column
        int size = offsets[rows - 1];
        Object[] keys = keyDataType.deserializeBinaryBulk(size, deserializer);
        Object[] values = valueDataType.deserializeBinaryBulk(size, deserializer);

        // populate Map of each row i
        for (int i = 0; i < rows; i++) {
            int offset = offsets[i];
            int offsetPrev = i > 0 ? offsets[i - 1] : 0;
            Map<Object, Object> rowMap = new HashMap<>(offset - offsetPrev);

            for (int j = offsetPrev; j < offset; j++) {
                rowMap.put(keys[j], values[j]);
            }

            maps[i] = rowMap;
        }

        return maps;
    }

    @Override
    public Map convertJdbcToJavaType(Object obj, ZoneId tz) throws ByteHouseSQLException {
        if (obj instanceof Map) {
            return (Map) obj;
        }
        throw new ByteHouseSQLException(-1, obj.getClass() + " cannot convert to " + Map.class);
    }

    /**
     * Converts map text representation (e.g. {'key1':1, 'key2':10}) into java Map
     */
    @Override
    public Map deserializeText(SQLLexer lexer) throws SQLException {
        ValidateUtils.isTrue(lexer.character() == '{', "expect '{' character for map opening");
        Map<Object, Object> map = new HashMap<>();

        while (!lexer.eof()) {
            if (lexer.isCharacter('}')) {
                lexer.character();
                break;
            }
            if (lexer.isCharacter(',')) {
                lexer.character();
            }
            Object key = keyDataType.deserializeText(lexer);
            ValidateUtils.isTrue(lexer.character() == ':', "expect key-value pair to be separated by ':'");
            Object value = valueDataType.deserializeText(lexer);

            map.put(key, value);
        }

        return map;
    }

    public IDataType getKeyDataType() {
        return keyDataType;
    }

    public IDataType getValueDataType() {
        return valueDataType;
    }

    @Override
    public Map[] allocate(int rows) {
        return new Map[rows];
    }
}
