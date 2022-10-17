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
package com.bytedance.bytehouse.buffer;

import java.io.IOException;

/**
 * read and buffer.
 */
public interface BuffedReader {

    /**
     * Read 4 bytes and return them as int
     */
    int readBinary() throws IOException;

    /**
     * read bytes into the array and return total amount of bytes read.
     *
     * @param bytes byte array container
     * @return amount of bytes read.
     */
    int readBinary(byte[] bytes) throws IOException;
}
