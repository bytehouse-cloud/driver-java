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

package com.bytedance.bytehouse.protocol;

import com.bytedance.bytehouse.serde.BinaryDeserializer;
import com.bytedance.bytehouse.exception.ByteHouseSQLException;
import java.io.IOException;
import java.sql.SQLException;

public class ExceptionResponse implements Response {

    public static SQLException readExceptionFrom(BinaryDeserializer deserializer) throws IOException {
        int code = deserializer.readInt();
        String name = deserializer.readUTF8StringBinary();
        String message = deserializer.readUTF8StringBinary();
        String stackTrace = deserializer.readUTF8StringBinary();

        if (deserializer.readBoolean()) {
            return new ByteHouseSQLException(
                    code, name + message + ". Stack trace:\n\n" + stackTrace, readExceptionFrom(deserializer));
        }

        return new ByteHouseSQLException(code, name + message + ". Stack trace:\n\n" + stackTrace);
    }

    @Override
    public ProtoType type() {
        return ProtoType.RESPONSE_EXCEPTION;
    }
}
