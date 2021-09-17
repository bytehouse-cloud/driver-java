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

import com.bytedance.bytehouse.buffer.ByteArrayWriter;
import com.bytedance.bytehouse.serde.BinarySerializer;
import com.bytedance.bytehouse.settings.BHConstants;
import java.io.IOException;
import java.util.List;

public class ColumnWriterBuffer {

    private final ByteArrayWriter columnWriter;

    public BinarySerializer column;

    public ColumnWriterBuffer() {
        this.columnWriter = new ByteArrayWriter(BHConstants.COLUMN_BUFFER_BYTES);
        this.column = new BinarySerializer(columnWriter, false);
    }

    public void writeTo(final BinarySerializer serializer) throws IOException {
        List<byte[]> byteBufferList = columnWriter.getBufferList();
        for (final byte[] bytes: byteBufferList) {
            serializer.writeBytes(bytes);
        }
    }
}
