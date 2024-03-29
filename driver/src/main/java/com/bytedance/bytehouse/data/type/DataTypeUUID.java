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
import java.util.UUID;

public class DataTypeUUID implements IDataType<UUID, String> {

    @Override
    public String name() {
        return "UUID";
    }

    @Override
    public int sqlTypeId() {
        return Types.VARCHAR;
    }

    @Override
    public UUID defaultValue() {
        return null;
    }

    @Override
    public Class<UUID> javaType() {
        return UUID.class;
    }

    @Override
    public Class<String> jdbcJavaType() {
        return String.class;
    }

    @Override
    public int getPrecision() {
        return 36;
    }

    @Override
    public int getScale() {
        return 0;
    }

    @Override
    public UUID deserializeText(SQLLexer lexer) throws SQLException {
        return UUID.fromString(lexer.stringLiteral());
    }

    @Override
    public void serializeBinary(UUID data, BinarySerializer serializer) throws SQLException, IOException {
        serializer.writeLong(data.getMostSignificantBits());
        serializer.writeLong(data.getLeastSignificantBits());
    }

    @Override
    public UUID deserializeBinary(BinaryDeserializer deserializer) throws SQLException, IOException {
        return new UUID(deserializer.readLong(), deserializer.readLong());
    }

    @Override
    public UUID convertJdbcToJavaType(Object obj, ZoneId tz) throws ByteHouseSQLException {
        if (obj instanceof UUID) {
            return (UUID) obj;
        }
        if (obj instanceof String) {
            return UUID.fromString((String) obj);
        }
        throw new ByteHouseSQLException(-1, obj.getClass() + " cannot convert to " + UUID.class);
    }

    @Override
    public UUID[] deserializeBinaryBulk(int rows, BinaryDeserializer deserializer) throws IOException, SQLException {
        UUID[] data = new UUID[rows];
        for (int row = 0; row < rows; row++) {
            data[row] = this.deserializeBinary(deserializer);
        }
        return data;
    }

    @Override
    public UUID[] allocate(int rows) {
        return new UUID[rows];
    }
}
