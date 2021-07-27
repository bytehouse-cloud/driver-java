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

import com.bytedance.bytehouse.client.NativeContext;
import com.bytedance.bytehouse.data.BlockSettings.Setting;
import com.bytedance.bytehouse.misc.Validate;
import com.bytedance.bytehouse.serde.BinaryDeserializer;
import com.bytedance.bytehouse.serde.BinarySerializer;

import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * A {@link Block} is like a mini table with all the columns and a subset of the rows.
 * A table is split into multiple blocks (with a max size) and sent across the network
 * <br><br>
 * {@link Block}s are used in select query: the driver will receive blocks from server.
 * also used in insert statements: the driver will send a block to the server representing the data to be inserted
 */
public class Block {

    private final IColumn[] columns;
    private final BlockSettings settings;

    private final Map<String, Integer> nameAndPositions; // position start with 1
    private final Object[] rowData; // transient data transfer storage
    private final int[] placeholderIndexes;
    private int rowCnt;

    public Block() {
        this(0, new IColumn[0]);
    }

    public Block(int rowCnt, IColumn[] columns) {
        this(rowCnt, columns, new BlockSettings(Setting.defaultValues()));
    }

    public Block(int rowCnt, IColumn[] columns, BlockSettings settings) {
        this.rowCnt = rowCnt;
        this.columns = columns;
        this.settings = settings;

        this.rowData = new Object[columns.length];
        this.nameAndPositions = new HashMap<>();
        this.placeholderIndexes = new int[columns.length];
        for (int i = 0; i < columns.length; i++) {
            nameAndPositions.put(columns[i].name(), i + 1);
            placeholderIndexes[i] = i;
        }
    }

    public static Block readFrom(BinaryDeserializer deserializer,
                                 NativeContext.ServerContext serverContext) throws IOException, SQLException {
        BlockSettings info = BlockSettings.readFrom(deserializer);

        int columnCnt = (int) deserializer.readVarInt();
        int rowCnt = (int) deserializer.readVarInt();

        IColumn[] columns = new IColumn[columnCnt];

        for (int i = 0; i < columnCnt; i++) {
            String name = deserializer.readUTF8StringBinary();
            String type = deserializer.readUTF8StringBinary();

            IDataType dataType = DataTypeFactory.get(type, serverContext);
            Object[] arr = dataType.deserializeBinaryBulk(rowCnt, deserializer);
            columns[i] = ColumnFactory.createColumn(name, dataType, arr);
        }

        return new Block(rowCnt, columns, info);
    }

    public int rowCnt() {
        return rowCnt;
    }

    public int columnCnt() {
        return columns.length;
    }

    public void appendRow() throws SQLException {
        int i = 0;
        try {
            for (; i < columns.length; i++) {
                columns[i].write(rowData[i]);
            }
            rowCnt++;
        } catch (IOException | ClassCastException | NullPointerException e) {
            throw new SQLException("Exception processing value " + rowData[i] + " for column: " + columns[i].name(), e);
        }
    }

    public void setObject(int columnIdx, Object object) {
        rowData[columnIdx] = object;
    }

    public int paramIdx2ColumnIdx(int paramIdx) {
        return placeholderIndexes[paramIdx];
    }

    public void incPlaceholderIndexes(int columnIdx) {
        for (int i = columnIdx; i < placeholderIndexes.length; i++) {
            placeholderIndexes[i] += 1;
        }
    }

    public void writeTo(BinarySerializer serializer) throws IOException, SQLException {
        settings.writeTo(serializer);

        serializer.writeVarInt(columns.length);
        serializer.writeVarInt(rowCnt);

        for (IColumn column : columns) {
            column.flushToSerializer(serializer, true);
        }
    }

    // idx start with 0
    public IColumn getColumn(int columnIdx) throws SQLException {
        Validate.isTrue(columnIdx < columns.length,
                "Position " + columnIdx +
                        " is out of bound in Block.getByPosition, max position = " + (columns.length - 1));
        return columns[columnIdx];
    }

    // position start with 1
    public int getPositionByName(String columnName) throws SQLException {
        Validate.isTrue(nameAndPositions.containsKey(columnName), "Column '" + columnName + "' does not exist");
        return nameAndPositions.get(columnName);
    }

    public Object getObject(int columnIdx) throws SQLException {
        Validate.isTrue(columnIdx < columns.length,
                "Position " + columnIdx +
                        " is out of bound in Block.getByPosition, max position = " + (columns.length - 1));
        return rowData[columnIdx];
    }

    public void initWriteBuffer() {
        for (IColumn column : columns) {
            column.setColumnWriterBuffer(new ColumnWriterBuffer());
        }
    }
}
