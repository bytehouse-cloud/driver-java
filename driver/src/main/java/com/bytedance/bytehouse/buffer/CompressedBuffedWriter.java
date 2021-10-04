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

import com.bytedance.bytehouse.misc.BytesHelper;
import com.bytedance.bytehouse.misc.ClickHouseCityHash;
import io.airlift.compress.Compressor;
import io.airlift.compress.lz4.Lz4Compressor;
import java.io.IOException;

import static com.bytedance.bytehouse.settings.BHConstants.CHECKSUM_LENGTH;
import static com.bytedance.bytehouse.settings.BHConstants.COMPRESSION_HEADER_LENGTH;

/**
 * {@link CompressedBuffedWriter} writes in a compressed format
 */
public class CompressedBuffedWriter implements BuffedWriter, BytesHelper {

    private final int capacity;

    private final byte[] writtenBuf;

    private final BuffedWriter writer;

    private final Compressor lz4Compressor = new Lz4Compressor();

    // no longer in use
    //private final Compressor zstdCompressor = new ZstdCompressor();

    private int position;

    /**
     * Constructor.
     */
    public CompressedBuffedWriter(final int capacity, final BuffedWriter writer) {
        this.capacity = capacity;
        this.writtenBuf = new byte[capacity];
        this.writer = writer;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeBinary(final byte byt) throws IOException {
        writtenBuf[position++] = byt;
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
        while (remaining() < length) {
            final int num = remaining();
            System.arraycopy(bytes, offset, writtenBuf, position, remaining());
            position += num;

            flushToTarget(false);
            offset += num;
            length -= num;
        }

        System.arraycopy(bytes, offset, writtenBuf, position, length);
        position += length;
        flushToTarget(false);
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
        if (position > 0 && (force || !hasRemaining())) {
            final int maxLen = lz4Compressor.maxCompressedLength(position);

            final byte[] compressedBuffer =
                    new byte[maxLen + COMPRESSION_HEADER_LENGTH + CHECKSUM_LENGTH];
            final int res = lz4Compressor.compress(
                    writtenBuf,
                    0,
                    position,
                    compressedBuffer,
                    COMPRESSION_HEADER_LENGTH + CHECKSUM_LENGTH,
                    compressedBuffer.length
            );

            compressedBuffer[CHECKSUM_LENGTH] = (byte) (0x82 & 0xFF);
            final int compressedSize = res + COMPRESSION_HEADER_LENGTH;
            System.arraycopy(
                    getBytesLE(compressedSize),
                    0,
                    compressedBuffer,
                    CHECKSUM_LENGTH + 1,
                    Integer.BYTES
            );
            System.arraycopy(
                    getBytesLE(position),
                    0,
                    compressedBuffer,
                    CHECKSUM_LENGTH + Integer.BYTES + 1,
                    Integer.BYTES
            );

            final long[] checksum = ClickHouseCityHash.cityHash128(
                    compressedBuffer,
                    CHECKSUM_LENGTH,
                    compressedSize
            );
            System.arraycopy(
                    getBytesLE(checksum[0]),
                    0,
                    compressedBuffer,
                    0,
                    Long.BYTES
            );
            System.arraycopy(
                    getBytesLE(checksum[1]),
                    0,
                    compressedBuffer,
                    Long.BYTES,
                    Long.BYTES
            );

            writer.writeBinary(compressedBuffer, 0, compressedSize + CHECKSUM_LENGTH);
            position = 0;
        }
    }

    private boolean hasRemaining() {
        return position < capacity;
    }

    private int remaining() {
        return capacity - position;
    }
}
