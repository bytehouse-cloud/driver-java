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

import com.bytedance.bytehouse.client.ServerContext;
import com.bytedance.bytehouse.data.IDataType;
import com.bytedance.bytehouse.data.type.SerializableCharset;
import com.bytedance.bytehouse.exception.ByteHouseSQLException;
import com.bytedance.bytehouse.misc.BytesCharSeq;
import com.bytedance.bytehouse.misc.SQLLexer;
import com.bytedance.bytehouse.misc.ValidateUtils;
import com.bytedance.bytehouse.serde.BinaryDeserializer;
import com.bytedance.bytehouse.serde.BinarySerializer;
import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.sql.Types;
import java.time.ZoneId;

public class DataTypeFixedString implements IDataType<CharSequence, String> {

    public static DataTypeCreator<CharSequence, String> creator = (lexer, serverContext) -> {
        ValidateUtils.isTrue(lexer.character() == '(');
        Number fixedStringN = lexer.numberLiteral();
        ValidateUtils.isTrue(lexer.character() == ')');
        return new DataTypeFixedString("FixedString(" + fixedStringN.intValue() + ")", fixedStringN.intValue(), serverContext);
    };

    private final int n;

    private final String name;

    private final String defaultValue;

    private final SerializableCharset serializableCharset;

    public DataTypeFixedString(String name, int n, ServerContext serverContext) {
        this.n = n;
        this.name = name;
        Charset charset = serverContext.getConfigure().charset();
        this.serializableCharset = new SerializableCharset(charset);

        byte[] data = new byte[n];
        for (int i = 0; i < n; i++) {
            data[i] = '\u0000';
        }
        this.defaultValue = new String(data, charset);
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public int sqlTypeId() {
        return Types.VARCHAR;
    }

    @Override
    public String defaultValue() {
        return defaultValue;
    }

    @Override
    public Class<CharSequence> javaType() {
        return CharSequence.class;
    }

    @Override
    public Class<String> jdbcJavaType() {
        return String.class;
    }

    @Override
    public int getPrecision() {
        return n;
    }

    @Override
    public int getScale() {
        return 0;
    }

    @Override
    public void serializeBinary(CharSequence data, BinarySerializer serializer) throws SQLException, IOException {
        if (data instanceof BytesCharSeq) {
            writeBytes((((BytesCharSeq) data).bytes()), serializer);
        } else {
            writeBytes(data.toString().getBytes(this.serializableCharset.get()), serializer);
        }
    }

    private void writeBytes(byte[] bs, BinarySerializer serializer) throws IOException, SQLException {
        byte[] res;
        if (bs.length > n) {
            throw new SQLException("The size of FixString column is too large, got " + bs.length);
        }
        if (bs.length == n) {
            res = bs;
        } else {
            res = new byte[n];
            System.arraycopy(bs, 0, res, 0, bs.length);
        }
        serializer.writeBytes(res);
    }

    @Override
    public String deserializeBinary(BinaryDeserializer deserializer) throws SQLException, IOException {
        byte[] bs = deserializer.readBytes(n);
        return new String(bs, this.serializableCharset.get());
    }

    @Override
    public CharSequence convertJdbcToJavaType(Object obj, ZoneId tz) throws ByteHouseSQLException {
        if (obj instanceof CharSequence) {
            return (CharSequence) obj;
        }
        if (obj instanceof byte[]) {
            return new BytesCharSeq((byte[]) obj);
        }
        return obj.toString();
    }

    @Override
    public CharSequence deserializeText(SQLLexer lexer) throws SQLException {
        return lexer.stringLiteral();
    }

    @Override
    public String[] getAliases() {
        return new String[]{"BINARY"};
    }

    @Override
    public CharSequence[] deserializeBinaryBulk(int rows, BinaryDeserializer deserializer) throws IOException, SQLException {
        CharSequence[] data = new CharSequence[rows];
        for (int row = 0; row < rows; row++) {
            data[row] = this.deserializeBinary(deserializer);
        }
        return data;
    }

    @Override
    public CharSequence[] allocate(int rows) {
        return new CharSequence[rows];
    }
}
