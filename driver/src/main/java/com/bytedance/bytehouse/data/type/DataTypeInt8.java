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

import com.bytedance.bytehouse.exception.ByteHouseSQLException;
import com.bytedance.bytehouse.misc.SQLLexer;
import com.bytedance.bytehouse.serde.BinaryDeserializer;
import com.bytedance.bytehouse.serde.BinarySerializer;
import java.io.IOException;
import java.sql.SQLException;
import java.time.ZoneId;

public class DataTypeInt8 implements BaseDataTypeInt8<Byte, Byte> {

    @Override
    public String name() {
        return "Int8";
    }

    @Override
    public Byte defaultValue() {
        return 0;
    }

    @Override
    public Class<Byte> javaType() {
        return Byte.class;
    }

    @Override
    public int getPrecision() {
        return 4;
    }

    @Override
    public void serializeBinary(Byte data, BinarySerializer serializer) throws SQLException, IOException {
        serializer.writeByte(data);
    }

    @Override
    public Byte deserializeBinary(BinaryDeserializer deserializer) throws IOException {
        return deserializer.readByte();
    }

    @Override
    public Byte convertJdbcToJavaType(Object obj, ZoneId tz) throws ByteHouseSQLException {
        if (obj instanceof Number) {
            return ((Number) obj).byteValue();
        }
        throw new ByteHouseSQLException(-1, obj.getClass() + " cannot convert to " + Byte.class);
    }

    @Override
    public String[] getAliases() {
        return new String[]{"TINYINT"};
    }

    @Override
    public Byte deserializeText(SQLLexer lexer) throws SQLException {
        return lexer.numberLiteral().byteValue();
    }

    @Override
    public boolean isSigned() {
        return true;
    }
}
