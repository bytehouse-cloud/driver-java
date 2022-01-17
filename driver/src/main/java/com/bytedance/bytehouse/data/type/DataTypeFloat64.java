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
package com.bytedance.bytehouse.data.type;

import com.bytedance.bytehouse.data.IDataType;
import com.bytedance.bytehouse.exception.ByteHouseSQLException;
import com.bytedance.bytehouse.misc.SQLLexer;
import com.bytedance.bytehouse.serde.BinaryDeserializer;
import com.bytedance.bytehouse.serde.BinarySerializer;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.Types;
import java.time.ZoneId;

public class DataTypeFloat64 implements IDataType<Double, Double> {

    @Override
    public String name() {
        return "Float64";
    }

    @Override
    public int sqlTypeId() {
        return Types.DOUBLE;
    }

    @Override
    public Double defaultValue() {
        return 0.0D;
    }

    @Override
    public Class<Double> javaType() {
        return Double.class;
    }

    @Override
    public int getPrecision() {
        return 17;
    }

    @Override
    public int getScale() {
        return 17;
    }

    @Override
    public void serializeBinary(Double data, BinarySerializer serializer) throws SQLException, IOException {
        serializer.writeDouble(data);
    }

    @Override
    public Double deserializeBinary(BinaryDeserializer deserializer) throws SQLException, IOException {
        return deserializer.readDouble();
    }

    @Override
    public Double convertJdbcToJavaType(Object obj, ZoneId tz) throws ByteHouseSQLException {
        if (obj instanceof Number) {
            return ((Number) obj).doubleValue();
        }
        if (obj instanceof String) {
            return Double.valueOf((String) obj);
        }
        throw new ByteHouseSQLException(-1, obj.getClass() + " cannot convert to " + Double.class);
    }

    @Override
    public String[] getAliases() {
        return new String[]{"DOUBLE"};
    }

    @Override
    public Double deserializeText(SQLLexer lexer) throws SQLException {
        return lexer.numberLiteral().doubleValue();
    }

    @Override
    public boolean isSigned() {
        return true;
    }

    @Override
    public Double[] deserializeBinaryBulk(int rows, BinaryDeserializer deserializer) throws IOException, SQLException {
        Double[] data = new Double[rows];
        for (int row = 0; row < rows; row++) {
            data[row] = this.deserializeBinary(deserializer);
        }
        return data;
    }

    @Override
    public Double[] allocate(int rows) {
        return new Double[rows];
    }
}
