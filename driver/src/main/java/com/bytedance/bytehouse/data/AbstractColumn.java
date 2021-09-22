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

public abstract class AbstractColumn implements IColumn {

    protected final String name;

    protected final IDataType<?, ?> type;

    // Note: values is only for reading
    protected Object[] values;

    protected ColumnWriterBuffer buffer;

    public AbstractColumn(String name, IDataType<?, ?> type, Object[] values) {
        this.name = name;
        this.type = type;
        this.values = values;
    }

    @Override
    public boolean isExported() {
        return name != null;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public IDataType type() {
        return type;
    }

    @Override
    public Object value(int idx) {
        return values[idx];
    }

    @Override
    public void clear() {
        values = new Object[0];
    }

    @Override
    public ColumnWriterBuffer getColumnWriterBuffer() {
        return buffer;
    }

    @Override
    public void setColumnWriterBuffer(ColumnWriterBuffer buffer) {
        this.buffer = buffer;
    }
}
