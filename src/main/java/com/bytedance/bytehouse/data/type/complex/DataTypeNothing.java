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

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Types;

import com.bytedance.bytehouse.client.NativeContext;
import com.bytedance.bytehouse.data.IDataType;
import com.bytedance.bytehouse.misc.SQLLexer;
import com.bytedance.bytehouse.serde.BinaryDeserializer;
import com.bytedance.bytehouse.serde.BinarySerializer;

public class DataTypeNothing implements IDataType<Object, Object> {

    public static DataTypeCreator<Object, Object> CREATOR =
            (lexer, serverContext) -> new DataTypeNothing(serverContext);

    public DataTypeNothing(NativeContext.ServerContext serverContext) {
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
    public Object defaultValue() {
        return null;
    }

    @Override
    public Class<Object> javaType() {
        return Object.class;
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
    }

    @Override
    public Object deserializeBinary(BinaryDeserializer deserializer) throws SQLException, IOException {
        return new Object();
    }

    @Override
    public String[] getAliases() {
        return new String[]{"NULL"};
    }

    @Override
    public Object deserializeText(SQLLexer lexer) throws SQLException {
        return lexer.stringView();
    }
}
