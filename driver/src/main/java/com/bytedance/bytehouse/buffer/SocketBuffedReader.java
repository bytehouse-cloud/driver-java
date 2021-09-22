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
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;

/**
 * read from socket and buffer into byte array.
 *
 * this class reads {@link BHConstants#SOCKET_RECV_BUFFER_BYTES} bytes into an internal
 * array and use the array to serve downstream. It will only fetch from inputStream
 * again if the internal buffer runs out.
 */
public class SocketBuffedReader implements BuffedReader {

    private final int capacity;

    private final byte[] buf;

    private final InputStream in;

    private int limit;

    private int position;

    /**
     * constructor.
     */
    public SocketBuffedReader(final Socket socket) throws IOException {
        this(socket.getInputStream(), BHConstants.SOCKET_RECV_BUFFER_BYTES);
    }

    SocketBuffedReader(
            final InputStream in,
            final int capacity
    ) {
        this.limit = 0;
        this.position = 0;
        this.capacity = capacity;

        this.in = in;
        this.buf = new byte[capacity];
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int readBinary() throws IOException {
        if (!remaining() && !refill()) {
            throw new EOFException("Attempt to read after eof.");
        }

        return buf[position++] & 0xFF;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("PMD.AvoidReassigningLoopVariables")
    public int readBinary(final byte[] bytes) throws IOException {
        for (int i = 0; i < bytes.length; ) {
            if (!remaining() && !refill()) {
                throw new EOFException("Attempt to read after eof.");
            }

            final int pending = bytes.length - i;
            final int fillLength = Math.min(pending, limit - position);

            if (fillLength > 0) {
                System.arraycopy(buf, position, bytes, i, fillLength);

                i += fillLength;
                this.position += fillLength;
            }
        }
        return bytes.length;
    }

    private boolean remaining() {
        return position < limit;
    }

    @SuppressWarnings("PMD.AssignmentInOperand")
    private boolean refill() throws IOException {
        if (!remaining() && (limit = in.read(buf, 0, capacity)) <= 0) {
            throw new EOFException("Attempt to read after eof.");
        }
        position = 0;
        return true;
    }
}
