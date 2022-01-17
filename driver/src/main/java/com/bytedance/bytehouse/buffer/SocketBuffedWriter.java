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

import com.bytedance.bytehouse.settings.BHConstants;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;

/**
 * {@link SocketBuffedWriter} directly writes into the outputStream of the socket.
 */
public class SocketBuffedWriter implements BuffedWriter {

    private final OutputStream out;
    private final int capacity;
    private final byte[] writtenBuf;
    private int position;

    /**
     * constructor.
     */
    public SocketBuffedWriter(final Socket socket) throws IOException {
        this(BHConstants.SOCKET_SEND_BUFFER_BYTES, socket);
    }

    public SocketBuffedWriter(final int capacity, final Socket socket) throws IOException {
        this.capacity = capacity;
        this.out = socket.getOutputStream();
        this.writtenBuf = new byte[capacity];
        this.position = 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeBinary(final byte byt) throws IOException {
        if (position == capacity) {
            flushToTarget(true);
        }
        writtenBuf[position++] = byt;
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
    public void writeBinary(
            final byte[] bytes,
            final int offset,
            final int length
    ) throws IOException {
        int currOffset = offset;
        int remainingLength = length;

        while (remaining() < remainingLength) {
            final int num = remaining();
            System.arraycopy(bytes, currOffset, writtenBuf, position, num);
            position += num;

            flushToTarget(true);
            currOffset += num;
            remainingLength -= num;
        }

        System.arraycopy(bytes, currOffset, writtenBuf, position, remainingLength);
        position += remainingLength;
    }

    @Override
    public void writeBinaryNow(final byte[] bytes) throws IOException {
        flushToTarget(true);
        out.write(bytes);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void flushToTarget(final boolean force) throws IOException {
        out.write(writtenBuf, 0, position);
        out.flush();
        this.position = 0;
    }

    private int remaining() {
        return capacity - position;
    }
}
