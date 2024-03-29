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
import com.bytedance.bytehouse.misc.ValidateUtils;
import com.bytedance.bytehouse.serde.BinaryDeserializer;
import com.bytedance.bytehouse.serde.BinarySerializer;
import java.io.IOException;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalDate;
import java.time.ZoneId;

public class DataTypeDate implements IDataType<LocalDate, Date> {

    private static final LocalDate DEFAULT_VALUE = LocalDate.ofEpochDay(0);

    public DataTypeDate() {
    }

    @Override
    public String name() {
        return "Date";
    }

    @Override
    public int sqlTypeId() {
        return Types.DATE;
    }

    @Override
    public LocalDate defaultValue() {
        return DEFAULT_VALUE;
    }

    @Override
    public Class<LocalDate> javaType() {
        return LocalDate.class;
    }

    @Override
    public Class<Date> jdbcJavaType() {
        return Date.class;
    }

    @Override
    public int getPrecision() {
        return 10;
    }

    @Override
    public int getScale() {
        return 0;
    }

    @Override
    public void serializeBinary(LocalDate data, BinarySerializer serializer) throws SQLException, IOException {
        long epochDay = data.toEpochDay();
        serializer.writeShort((short) epochDay);
    }

    @Override
    public LocalDate deserializeBinary(BinaryDeserializer deserializer) throws IOException {
        short epochDay = deserializer.readShort();
        return LocalDate.ofEpochDay(epochDay);
    }

    @Override
    public LocalDate convertJdbcToJavaType(Object obj, ZoneId tz) throws ByteHouseSQLException {
        if (obj instanceof java.util.Date) {
            return ((Date) obj).toLocalDate();
        }
        if (obj instanceof LocalDate) {
            return (LocalDate) obj;
        }
        if (obj instanceof String) {
            return Date.valueOf((String) obj).toLocalDate();
        }
        throw new ByteHouseSQLException(-1, obj.getClass() + " cannot convert to " + LocalDate.class);
    }

    @Override
    public String[] getAliases() {
        return new String[0];
    }

    @Override
    public LocalDate deserializeText(SQLLexer lexer) throws SQLException {
        ValidateUtils.isTrue(lexer.character() == '\'');
        int year = lexer.numberLiteral().intValue();
        ValidateUtils.isTrue(lexer.character() == '-');
        int month = lexer.numberLiteral().intValue();
        ValidateUtils.isTrue(lexer.character() == '-');
        int day = lexer.numberLiteral().intValue();
        ValidateUtils.isTrue(lexer.character() == '\'');

        return LocalDate.of(year, month, day);
    }

    @Override
    public LocalDate[] deserializeBinaryBulk(int rows, BinaryDeserializer deserializer) throws IOException, SQLException {
        LocalDate[] data = new LocalDate[rows];
        for (int row = 0; row < rows; row++) {
            data[row] = this.deserializeBinary(deserializer);
        }
        return data;
    }

    @Override
    public LocalDate[] allocate(int rows) {
        return new LocalDate[rows];
    }
}
