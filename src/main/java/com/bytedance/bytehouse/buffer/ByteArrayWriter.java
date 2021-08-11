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
package com.bytedance.bytehouse.buffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

/**
 * writes to a List of JVM-memory-backed {@link ByteBuffer}.
 * the content is then access by the getter method of the list.
 */
public class ByteArrayWriter implements BuffedWriter {

    private final int blockSize;

    private final List<ByteBuffer> byteBufferList = new LinkedList<>();

    private ByteBuffer buffer;

    /**
     * Create a {@link ByteBuffer} with block size.
     */
    public ByteArrayWriter(final int blockSizeInByte) {
        this.blockSize = blockSizeInByte;
        this.buffer = ByteBuffer.allocate(blockSizeInByte);
        this.byteBufferList.add(buffer);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeBinary(final byte byt) throws IOException {
        buffer.put(byt);
        flushToTarget(false);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeBinary(final byte[] bytes) throws IOException {
        writeBinary(bytes, 0, bytes.length);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("PMD.AvoidReassigningParameters")
    public void writeBinary(
            final byte[] bytes,
            int offset,
            int length
    ) throws IOException {

        while (buffer.remaining() < length) {
            final int num = buffer.remaining();
            buffer.put(bytes, offset, num);
            flushToTarget(true);

            offset += num;
            length -= num;
        }

        buffer.put(bytes, offset, length);
        flushToTarget(false);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void flushToTarget(final boolean force) throws IOException {
        if (buffer.hasRemaining() && !force) {
            return;
        }
        // the current buffer is already added to the list. Hence we can directly dereference
        buffer = ByteBuffer.allocate(blockSize);
        byteBufferList.add(buffer);
    }

    /**
     * Get the accumulated content.
     */
    public List<ByteBuffer> getBufferList() {
        return byteBufferList;
    }
}
