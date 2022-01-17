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

import com.bytedance.bytehouse.serde.BinarySerializer;
import java.io.IOException;
import java.sql.SQLException;

public interface IColumn {

    boolean isExported();

    String name();

    IDataType<?, ?> type();

    Object value(int idx);

    void write(Object object) throws IOException, SQLException;

    /**
     * Flush to socket output stream
     *
     * @param serializer is serializer wrapper of tcp socket
     * @param now        means we should flush all the buffer to serializer now
     */
    void flushToSerializer(BinarySerializer serializer, boolean now) throws IOException, SQLException;

    void clear();

    ColumnWriterBuffer getColumnWriterBuffer();

    void setColumnWriterBuffer(ColumnWriterBuffer buffer);

    void reuseColumnWriterBuffer();
}


