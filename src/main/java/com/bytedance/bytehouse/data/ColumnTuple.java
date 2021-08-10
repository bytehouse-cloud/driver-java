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
package com.bytedance.bytehouse.data;

import com.bytedance.bytehouse.data.type.complex.DataTypeTuple;
import com.bytedance.bytehouse.jdbc.ByteHouseStruct;
import com.bytedance.bytehouse.serde.BinarySerializer;
import java.io.IOException;
import java.sql.SQLException;

public class ColumnTuple extends AbstractColumn {

    // data represents nested column in ColumnArray
    private final IColumn[] columnDataArray;

    public ColumnTuple(String name, DataTypeTuple type, Object[] values) {
        super(name, type, values);

        IDataType<?, ?>[] types = type.getNestedTypes();
        columnDataArray = new IColumn[types.length];
        for (int i = 0; i < types.length; i++) {
            columnDataArray[i] = ColumnFactory.createColumn(null, types[i], null);
        }
    }

    @Override
    public void write(Object object) throws IOException, SQLException {
        ByteHouseStruct tuple = (ByteHouseStruct) object;
        for (int i = 0; i < columnDataArray.length; i++) {
            columnDataArray[i].write(tuple.getAttributes()[i]);
        }
    }

    @Override
    public void flushToSerializer(BinarySerializer serializer, boolean now) throws SQLException, IOException {
        if (isExported()) {
            serializer.writeUTF8StringBinary(name);
            serializer.writeUTF8StringBinary(type.name());
        }

        // we should to flush all the nested data to serializer
        // because they are using separate buffers.
        for (IColumn data : columnDataArray) {
            data.flushToSerializer(serializer, true);
        }

        if (now) {
            buffer.writeTo(serializer);
        }
    }

    @Override
    public void setColumnWriterBuffer(ColumnWriterBuffer buffer) {
        super.setColumnWriterBuffer(buffer);

        for (IColumn data : columnDataArray) {
            data.setColumnWriterBuffer(new ColumnWriterBuffer());
        }
    }

    @Override
    public void clear() {
    }
}
