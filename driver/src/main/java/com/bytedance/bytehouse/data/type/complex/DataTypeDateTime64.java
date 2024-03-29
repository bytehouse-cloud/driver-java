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
import com.bytedance.bytehouse.misc.DateTimeUtil;
import com.bytedance.bytehouse.misc.SQLLexer;
import com.bytedance.bytehouse.misc.ValidateUtils;
import com.bytedance.bytehouse.serde.BinaryDeserializer;
import com.bytedance.bytehouse.serde.BinarySerializer;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

public class DataTypeDateTime64 implements IDataType<ZonedDateTime, Timestamp> {

    public static final int NANOS_IN_SECOND = 1_000_000_000;

    public static final int MILLIS_IN_SECOND = 1000;

    public static final int[] POW_10 = {1, 10, 100, 1_000, 10_000, 100_000, 1_000_000, 10_000_000, 100_000_000, 1_000_000_000};

    public static final int MIN_SCALE = 0;

    public static final int MAX_SCALA = 9;

    public static final int DEFAULT_SCALE = 3;

    private static final LocalDateTime EPOCH_LOCAL_DT = LocalDateTime.of(1970, 1, 1, 0, 0);

    public static DataTypeCreator<ZonedDateTime, Timestamp> creator = (lexer, serverContext) -> {
        if (lexer.isCharacter('(')) {
            ValidateUtils.isTrue(lexer.character() == '(');
            int scale = lexer.numberLiteral().intValue();
            ValidateUtils.isTrue(scale >= DataTypeDateTime64.MIN_SCALE && scale <= DataTypeDateTime64.MAX_SCALA,
                    "scale=" + scale + " out of range [" + DataTypeDateTime64.MIN_SCALE + "," + DataTypeDateTime64.MAX_SCALA + "]");
            if (lexer.isCharacter(',')) {
                ValidateUtils.isTrue(lexer.character() == ',');
                String dataTimeZone = lexer.stringLiteral();
                ValidateUtils.isTrue(lexer.character() == ')');
                return new DataTypeDateTime64("DateTime64(" + scale + ", '" + dataTimeZone + "')", scale, serverContext);
            }

            ValidateUtils.isTrue(lexer.character() == ')');
            return new DataTypeDateTime64("DateTime64(" + scale + ")", scale, serverContext);
        }
        return new DataTypeDateTime64("DateTime64", DataTypeDateTime64.DEFAULT_SCALE, serverContext);
    };

    private final String name;

    private final int scale;

    private final ZoneId tz;

    private final ZonedDateTime defaultValue;

    public DataTypeDateTime64(String name, int scala, ServerContext serverContext) {
        this.name = name;
        this.scale = scala;
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
        return 20;
    }

    @Override
    public int getScale() {
        return scale;
    }

    @Override
    public ZonedDateTime deserializeText(SQLLexer lexer) throws SQLException {
        ValidateUtils.isTrue(lexer.character() == '\'');
        int year = lexer.numberLiteral().intValue();
        ValidateUtils.isTrue(lexer.character() == '-');
        int month = lexer.numberLiteral().intValue();
        ValidateUtils.isTrue(lexer.character() == '-');
        int day = lexer.numberLiteral().intValue();
        ValidateUtils.isTrue(lexer.isWhitespace());
        int hours = lexer.numberLiteral().intValue();
        ValidateUtils.isTrue(lexer.character() == ':');
        int minutes = lexer.numberLiteral().intValue();
        ValidateUtils.isTrue(lexer.character() == ':');
        BigDecimal seconds = BigDecimal.valueOf(lexer.numberLiteral().doubleValue())
                .setScale(scale, BigDecimal.ROUND_HALF_UP);
        int second = seconds.intValue();
        int nanos = seconds.subtract(BigDecimal.valueOf(second)).movePointRight(9).intValue();
        ValidateUtils.isTrue(lexer.character() == '\'');

        return ZonedDateTime.of(year, month, day, hours, minutes, second, nanos, tz);
    }

    @Override
    public void serializeBinary(ZonedDateTime data, BinarySerializer serializer) throws IOException {
        long epochSeconds = DateTimeUtil.toEpochSecond(data);
        int nanos = data.getNano();
        long value = (epochSeconds * NANOS_IN_SECOND + nanos) / POW_10[MAX_SCALA - scale];
        serializer.writeLong(value);
    }

    @Override
    public ZonedDateTime deserializeBinary(BinaryDeserializer deserializer) throws IOException {
        long value = deserializer.readLong() * POW_10[MAX_SCALA - scale];
        long epochSeconds = value / NANOS_IN_SECOND;
        int nanos = (int) (value % NANOS_IN_SECOND);

        return DateTimeUtil.toZonedDateTime(epochSeconds, nanos, tz);
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
        if (obj instanceof String) {
            Timestamp timestamp = Timestamp.valueOf((String) obj);
            return DateTimeUtil.toZonedDateTime(timestamp, tz);
        }
        throw new ByteHouseSQLException(-1, obj.getClass() + " cannot convert to " + ZonedDateTime.class);
    }

    @Override
    public ZonedDateTime[] deserializeBinaryBulk(int rows, BinaryDeserializer deserializer) throws IOException, SQLException {
        ZonedDateTime[] data = new ZonedDateTime[rows];
        for (int row = 0; row < rows; row++) {
            data[row] = this.deserializeBinary(deserializer);
        }
        return data;
    }

    @Override
    public ZonedDateTime[] allocate(int rows) {
        return new ZonedDateTime[rows];
    }
}
