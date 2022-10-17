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

import com.bytedance.bytehouse.settings.ByteHouseErrCode;

/**
 * Type for transaction label already exists exception.
 */
public class TransactionLabelExistsException extends ByteHouseSQLException {

    private static final long serialVersionUID = 1;

    public TransactionLabelExistsException(final String message) {
        super(ByteHouseErrCode.INSERTION_LABEL_ALREADY_EXISTS.code(), message);
    }

    public TransactionLabelExistsException(final String message, final Throwable cause) {
        super(ByteHouseErrCode.INSERTION_LABEL_ALREADY_EXISTS.code(), message, cause);
    }
}
