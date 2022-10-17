/*
 * This file may have been modified by ByteDance Ltd. and/or its affiliates.
 *
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

/**
 * Use {@link ByteHouseException} internal, wrapped with {@link java.sql.SQLException} only on JDBC interfaces.
 * throw unchecked exception rather than checked exception.
 * <p>
 * Please avoid using CheckedException internal. See detail at <a>https://www.artima.com/intv/handcuffs.html</a>.
 */
public class ByteHouseException extends RuntimeException {
    private static final long serialVersionUID = 1;

    protected int errCode;

    public ByteHouseException(int errCode, String message) {
        super(message);
        this.errCode = errCode;
    }

    public ByteHouseException(int errCode, String message, Throwable cause) {
        super(message, cause);
        this.errCode = errCode;
    }

    public ByteHouseException(int errCode, Throwable cause) {
        super(cause);
        this.errCode = errCode;
    }

    public int errCode() {
        return errCode;
    }
}
