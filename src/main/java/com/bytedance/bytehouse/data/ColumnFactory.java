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

import com.bytedance.bytehouse.data.type.complex.DataTypeArray;
import com.bytedance.bytehouse.data.type.complex.DataTypeNullable;
import com.bytedance.bytehouse.data.type.complex.DataTypeTuple;

public class ColumnFactory {

    public static IColumn createColumn(String name, IDataType<?, ?> type, Object[] values) {
        if (type instanceof DataTypeArray)
            return new ColumnArray(name, (DataTypeArray) type, values);

        if (type instanceof DataTypeNullable)
            return new ColumnNullable(name, (DataTypeNullable) type, values);

        if (type instanceof DataTypeTuple)
            return new ColumnTuple(name, (DataTypeTuple) type, values);

        return new Column(name, type, values);
    }
}
