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

    private List<byte[]> byteBufferList = new LinkedList<>();

    private final int byteBufferCapacity;

    private int byteBufferCurrentSize;

    private int byteBufferPtr;

    private byte[] byteBuffer;

    private boolean byteBufferMaxSizeReached;

    /**
     * Create a {@link ByteBuffer} with block size.
     */
    public ByteArrayWriter(final int byteBufferCapacity) {
        this.byteBufferCapacity = byteBufferCapacity;
        this.byteBufferCurrentSize = 1;
        this.byteBufferPtr = 0;
        this.byteBuffer = new byte[byteBufferCurrentSize];
        this.byteBufferMaxSizeReached = false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeBinary(final byte byt) {
        if (remaining() == 0) {
            resizeByteArray();
        }
        byteBuffer[byteBufferPtr++] = byt;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeBinary(final byte[] bytes) {
        writeBinary(bytes, 0, bytes.length);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeBinary(final byte[] bytes, final int offset, final int length) {
        int currOffset = offset;
        int remainingLength = length;

        while (remaining() < remainingLength) {
            final int num = remaining();
            System.arraycopy(bytes, currOffset, byteBuffer, byteBufferPtr, num);
            byteBufferPtr += num;
            currOffset += num;
            remainingLength -= num;
            resizeByteArray();
        }

        System.arraycopy(bytes, currOffset, byteBuffer, byteBufferPtr, remainingLength);
        byteBufferPtr += remainingLength;
    }

    @Override
    public void writeBinaryNow(final byte[] bytes) throws IOException {
        flushToTarget(true);
        writeBinary(bytes);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void flushToTarget(final boolean force) throws IOException {
        if (force) {
            byte[] bytes = new byte[byteBufferPtr];
            System.arraycopy(byteBuffer, 0, bytes, 0, byteBufferPtr);
            byteBufferList.add(bytes);
        }
    }

    /**
     * Get the accumulated content.
     */
    public List<byte[]> getBufferList() throws IOException {
        flushToTarget(true);
        return byteBufferList;
    }

    private int remaining() {
        return byteBufferCurrentSize - byteBufferPtr;
    }

    private void resizeByteArray() {
        if (byteBufferCurrentSize * 2 > byteBufferCapacity) {
            byteBufferMaxSizeReached = true;
        }

        if (byteBufferMaxSizeReached) {
            byte[] bytes = new byte[byteBufferPtr];
            System.arraycopy(byteBuffer, 0, bytes, 0, byteBufferPtr);
            byteBufferList.add(bytes);

            byteBufferPtr = 0;
        }
        else {
            byte[] bytes = new byte[byteBufferCurrentSize*2];
            System.arraycopy(byteBuffer, 0, bytes, 0, byteBufferCurrentSize);
            byteBuffer = bytes;
            byteBufferCurrentSize = byteBufferCurrentSize*2;
        }
    }

    public void reuseByteArray() {
        byteBufferList.clear();
        byteBufferPtr = 0;
    }
}
