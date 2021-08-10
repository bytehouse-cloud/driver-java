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

import com.bytedance.bytehouse.data.type.complex.DataTypeMap;
import com.bytedance.bytehouse.serde.BinarySerializer;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A custom Column class to be used with the Map data type.
 *
 * This class modifies Column to track the offsets array (refer to {@link DataTypeMap} to understand its purpose) for
 * writing to the server. It also helps pack the keys and values for all the rows together for writing.
 */
public class ColumnMap extends AbstractColumn {

    private final List<Long> offsets;

    /**
     * Column for packing the keys for all the rows together for writing.
     */
    private final IColumn keysColumn;

    /**
     * Column for packing the values for all the rows together for writing.
     */
    private final IColumn valuesColumn;

    public ColumnMap(String name, DataTypeMap type, Object[] values) {
        super(name, type, values);
        offsets = new ArrayList<>();
        keysColumn = ColumnFactory.createColumn(null, type.getKeyDataType(), null);
        valuesColumn = ColumnFactory.createColumn(null, type.getValueDataType(), null);
    }

    /**
     * Appends a row with Map data to this column.
     */
    @Override
    public void write(Object object) throws IOException, SQLException {
        Map map = (Map) object;
        int size = map.size();

        // increment offsets
        offsets.add(offsets.isEmpty() ? size : offsets.get(offsets.size() - 1) + size);
        // add keys and values to their respective columns
        for (Object key : map.keySet()) {
            keysColumn.write(key);
            valuesColumn.write(map.get(key));
        }
    }

    /**
     * Flush data for all the rows to serializer.
     */
    @Override
    public void flushToSerializer(BinarySerializer serializer, boolean now) throws SQLException, IOException {
        if (isExported()) {
            serializer.writeUTF8StringBinary(name);
            serializer.writeUTF8StringBinary(type.name());
        }

        // write offsets
        flushOffsets(serializer);
        // write keys for all the rows
        keysColumn.flushToSerializer(serializer, true);
        // write values for all the rows
        valuesColumn.flushToSerializer(serializer, true);
    }

    /**
     * setColumnWriterBuffer() is called by Block to initialize the column for writing.
     * We use separate buffers for keys and values because they are to be written in order (keys first, then values)
     * to the serializer.
     */
    @Override
    public void setColumnWriterBuffer(ColumnWriterBuffer buffer) {
        super.setColumnWriterBuffer(buffer);
        keysColumn.setColumnWriterBuffer(new ColumnWriterBuffer());
        valuesColumn.setColumnWriterBuffer(new ColumnWriterBuffer());
    }

    @Override
    public void clear() {
        offsets.clear();
        keysColumn.clear();
        valuesColumn.clear();
    }

    private void flushOffsets(BinarySerializer serializer) throws IOException {
        for (long offset : offsets) {
            serializer.writeLong(offset);
        }
    }
}
