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

import com.bytedance.bytehouse.client.NativeContext;
import com.bytedance.bytehouse.data.IDataType;
import com.bytedance.bytehouse.exception.ByteHouseSQLException;
import com.bytedance.bytehouse.misc.DateTimeUtil;
import com.bytedance.bytehouse.misc.SQLLexer;
import com.bytedance.bytehouse.misc.Validate;
import com.bytedance.bytehouse.serde.BinaryDeserializer;
import com.bytedance.bytehouse.serde.BinarySerializer;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

public class DataTypeDateTime implements IDataType<ZonedDateTime, Timestamp> {

    private static final LocalDateTime EPOCH_LOCAL_DT = LocalDateTime.of(1970, 1, 1, 0, 0);

    public static DataTypeCreator<ZonedDateTime, Timestamp> creator = (lexer, serverContext) -> {
        if (lexer.isCharacter('(')) {
            Validate.isTrue(lexer.character() == '(');
            String dataTimeZone = lexer.stringLiteral();
            Validate.isTrue(lexer.character() == ')');
            return new DataTypeDateTime("DateTime('" + dataTimeZone + "')", serverContext);
        }
        return new DataTypeDateTime("DateTime", serverContext);
    };

    private final String name;

    private final ZoneId tz;

    private final ZonedDateTime defaultValue;

    public DataTypeDateTime(String name, NativeContext.ServerContext serverContext) {
        this.name = name;
        this.tz = DateTimeUtil.chooseTimeZone(serverContext);
        this.defaultValue = EPOCH_LOCAL_DT.atZone(tz);
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public int sqlTypeId() {
        return Types.TIMESTAMP;
    }

    @Override
    public ZonedDateTime defaultValue() {
        return defaultValue;
    }

    @Override
    public Class<ZonedDateTime> javaType() {
        return ZonedDateTime.class;
    }

    @Override
    public Class<Timestamp> jdbcJavaType() {
        return Timestamp.class;
    }

    @Override
    public int getPrecision() {
        return 0;
    }

    @Override
    public int getScale() {
        return 10;
    }

    @Override
    public ZonedDateTime deserializeText(SQLLexer lexer) throws SQLException {
        Validate.isTrue(lexer.character() == '\'');
        int year = lexer.numberLiteral().intValue();
        Validate.isTrue(lexer.character() == '-');
        int month = lexer.numberLiteral().intValue();
        Validate.isTrue(lexer.character() == '-');
        int day = lexer.numberLiteral().intValue();
        Validate.isTrue(lexer.isWhitespace());
        int hours = lexer.numberLiteral().intValue();
        Validate.isTrue(lexer.character() == ':');
        int minutes = lexer.numberLiteral().intValue();
        Validate.isTrue(lexer.character() == ':');
        int seconds = lexer.numberLiteral().intValue();
        Validate.isTrue(lexer.character() == '\'');

        return ZonedDateTime.of(year, month, day, hours, minutes, seconds, 0, tz);
    }

    @Override
    public void serializeBinary(ZonedDateTime data, BinarySerializer serializer) throws SQLException, IOException {
        serializer.writeInt((int) DateTimeUtil.toEpochSecond(data));
    }

    @Override
    public ZonedDateTime deserializeBinary(BinaryDeserializer deserializer) throws SQLException, IOException {
        int epochSeconds = deserializer.readInt();
        return DateTimeUtil.toZonedDateTime(epochSeconds, 0, tz);
    }

    @Override
    public ZonedDateTime convertJdbcToJavaType(Object obj, ZoneId tz) throws ByteHouseSQLException {
        if (obj instanceof Timestamp) {
            return DateTimeUtil.toZonedDateTime((Timestamp) obj, tz);
        }
        if (obj instanceof LocalDateTime) {
            return ((LocalDateTime) obj).atZone(tz);
        }
        if (obj instanceof ZonedDateTime) {
            return (ZonedDateTime) obj;
        }
        throw new ByteHouseSQLException(-1, obj.getClass() + " cannot convert to " + ZonedDateTime.class);
    }

    @Override
    public String[] getAliases() {
        return new String[]{"TIMESTAMP"};
    }
}
