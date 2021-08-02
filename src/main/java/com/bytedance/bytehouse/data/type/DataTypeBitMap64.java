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
import com.bytedance.bytehouse.jdbc.ByteHouseArray;
import com.bytedance.bytehouse.misc.BytesHelper;
import com.bytedance.bytehouse.misc.SQLLexer;
import com.bytedance.bytehouse.misc.Validate;
import com.bytedance.bytehouse.serde.BinaryDeserializer;
import com.bytedance.bytehouse.serde.BinarySerializer;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Array;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Support for BitMap64 data type - type specific to CNCH.<br/>
 * Equivalent code in driver-go can be found here
 * <a href="https://code.byted.org/bytehouse/driver-go/blob/main/driver/lib/data/column/bitmap.go">
 *     bitmap.go
 * </a>
 */
public class DataTypeBitMap64 implements IDataType<ByteHouseArray, Array>, BytesHelper {

    private static final DataTypeUInt64 DATA_TYPE_UINT_64 = new DataTypeUInt64();

    @Override
    public String name() {
        return "BitMap64";
    }

    @Override
    public int sqlTypeId() {
        return Types.ARRAY;
    }

    @Override
    public ByteHouseArray defaultValue() {
        return new ByteHouseArray(DATA_TYPE_UINT_64, new Object[0]);
    }

    @Override
    public Class<ByteHouseArray> javaType() {
        return ByteHouseArray.class;
    }

    @Override
    public Class<Array> jdbcJavaType() {
        return Array.class;
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
     * Serializes UInt64 array into BitMap64 byte stream.
     */
    @Override
    public void serializeBinary(ByteHouseArray data, BinarySerializer serializer) throws SQLException, IOException {

        // Each UInt64 in the UInt64 array (data) will be split into its 32 most significant bits, and 32 least
        // significant bits. In the HashMap, the MSB will form the key, and the LSB will form one of the values.
        Map<Integer, List<Integer>> msbLsbMap = new HashMap<>();
        Object[] uInt64Array = data.getArray();
        for (Object o : uInt64Array) {
            long value = ((BigInteger) o).longValue();
            int msb = (int) (value >> 32);
            int lsb = (int) value;

            msbLsbMap.computeIfAbsent(msb, k -> new ArrayList<>()).add(lsb);
        }

        try (ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream()) {
            try (DataOutputStream outputStream = new DataOutputStream(byteOutputStream)) {

                long mapSize = msbLsbMap.size();
                // write map size as UInt64 in little endian.
                outputStream.write(getBytesLE(mapSize));

                for (int msb : msbLsbMap.keySet()) {
                    // write msb as UInt32 in little endian.
                    outputStream.write(getBytesLE(msb));

                    // convert lsbValues to bitmap and serialize it
                    List<Integer> lsbValues = msbLsbMap.get(msb);
                    ImmutableRoaringBitmap bitmap = ImmutableRoaringBitmap.bitmapOf(
                            lsbValues.stream().mapToInt(i -> i).toArray()
                    );
                    bitmap.serialize(outputStream);
                }
                outputStream.flush();

                byte[] byteOutput = byteOutputStream.toByteArray();

                // write total length of data in bytes
                serializer.writeVarInt(byteOutput.length);
                // write all the byte output
                serializer.writeBytes(byteOutput);
            }
        }
    }

    /**
     * Deserializes byte stream of BitMap64 into a UInt64 array.
     */
    @Override
    public ByteHouseArray deserializeBinary(BinaryDeserializer deserializer) throws SQLException, IOException {
        // Get total length of data in bytes.
        // Casting long to int -> assumes that dataBytesLen will never exceed Integer.MAX_VALUE.
        // Integer.MAX_VALUE bytes = 2.15 GB
        int dataBytesLen = Math.toIntExact(deserializer.readVarInt());

        // wraps remaining data bytes in a ByteBuffer. Change order to LITTLE_ENDIAN, which corresponds to how
        // serializer/deserializer is writing/reading data.
        ByteBuffer byteBuffer = ByteBuffer.wrap(deserializer.readBytes(dataBytesLen));
        byteBuffer.order(ByteOrder.LITTLE_ENDIAN);

        // Using long as UInt64 here
        long mapSize = byteBuffer.getLong();
        List<BigInteger> result = new ArrayList<>();

        // refer to how it is being serialized to understand how to interpret this
        for (long i = 0; i < mapSize; i++) {
            long msb = byteBuffer.getInt() & 0xFFFFFFFFL;
            // read all lsb values for this msb
            ImmutableRoaringBitmap bitmap = new ImmutableRoaringBitmap(byteBuffer);
            // advance position in byteBuffer
            byteBuffer.position(byteBuffer.position() + bitmap.serializedSizeInBytes());

            bitmap.stream().forEach(lsb -> {
                // combine msb and lsb to form UInt64 value
                long value = (msb << 32) | (lsb & 0xFFFFFFFFL);
                result.add(new BigInteger(1, getBytes(value)));
            });
        }

        return new ByteHouseArray(DATA_TYPE_UINT_64, result.toArray());
    }

    /**
     * Converts bitmap text representation (e.g. [1,2,3,4,5]) into UInt64 array
     * Limitation: UInt64 cannot exceed Long.MAX_VALUE because of bug with UInt64 type deserializeText
     */
    @Override
    public ByteHouseArray deserializeText(SQLLexer lexer) throws SQLException {
        Validate.isTrue(lexer.character() == '[');
        List<BigInteger> uInt64List = new ArrayList<>();
        while (true) {
            if (lexer.isCharacter(']')) {
                lexer.character();
                break;
            }
            if (lexer.isCharacter(',')) {
                lexer.character();
            }
            uInt64List.add(DATA_TYPE_UINT_64.deserializeText(lexer));
        }
        return new ByteHouseArray(DATA_TYPE_UINT_64, uInt64List.toArray());
    }
}
