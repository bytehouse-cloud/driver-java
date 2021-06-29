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

package com.bytedance.bytehouse.exception;

import com.bytedance.bytehouse.settings.ByteHouseErrCode;

public class ByteHouseClientException extends ByteHouseException {

    public ByteHouseClientException(String message) {
        super(ByteHouseErrCode.CLIENT_ERROR.code(), message);
    }

    public ByteHouseClientException(String message, Throwable cause) {
        super(ByteHouseErrCode.CLIENT_ERROR.code(), message, cause);
    }

    public ByteHouseClientException(Throwable cause) {
        super(ByteHouseErrCode.CLIENT_ERROR.code(), cause);
    }
}
