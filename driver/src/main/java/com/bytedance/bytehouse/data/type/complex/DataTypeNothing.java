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

import com.bytedance.bytehouse.client.ServerContext;
import com.bytedance.bytehouse.data.IDataType;
import com.bytedance.bytehouse.exception.ByteHouseSQLException;
import com.bytedance.bytehouse.misc.SQLLexer;
import com.bytedance.bytehouse.serde.BinaryDeserializer;
import com.bytedance.bytehouse.serde.BinarySerializer;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.Types;
import java.time.ZoneId;

/**
 * Nothing data type represents cases where a value is not expected from the server's response.
 * You can't create/write a Nothing type value.
 */
public class DataTypeNothing implements IDataType<Byte, Object> {

    public static final DataTypeCreator<Byte, Object> CREATOR =
            (lexer, serverContext) -> new DataTypeNothing(serverContext);

    public DataTypeNothing(ServerContext serverContext) {
    }

    @Override
    public String name() {
        return "Nothing";
    }

    @Override
    public int sqlTypeId() {
        return Types.NULL;
    }

    @Override
    public Byte defaultValue() {
        return null;
    }

    @Override
    public Class<Byte> javaType() {
        return Byte.class;
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
     * Should not be able to write a Nothing type value.
     */
    @Override
    public void serializeBinary(Byte data, BinarySerializer serializer) throws SQLException, IOException {
        throw new SQLException("serializeBinary should not be called for Nothing type.");
    }

    /**
     * Nothing data type still has to read one byte when deserializing the response from server.
     */
    @Override
    public Byte deserializeBinary(BinaryDeserializer deserializer) throws SQLException, IOException {
        return deserializer.readByte();
    }

    @Override
    public Byte convertJdbcToJavaType(Object obj, ZoneId tz) throws ByteHouseSQLException {
        return null;
    }

    @Override
    public String[] getAliases() {
        return new String[]{"NULL"};
    }

    /**
     * Should not be able to create a Nothing type value from SQL statements.
     */
    @Override
    public Byte deserializeText(SQLLexer lexer) throws SQLException {
        throw new SQLException("deserializeText should not be called for Nothing type.");
    }

    @Override
    public Byte[] deserializeBinaryBulk(int rows, BinaryDeserializer deserializer) throws IOException, SQLException {
        Byte[] data = new Byte[rows];
        for (int row = 0; row < rows; row++) {
            data[row] = this.deserializeBinary(deserializer);
        }
        return data;
    }

    @Override
    public Byte[] allocate(int rows) {
        return new Byte[rows];
    }
}
