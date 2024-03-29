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

import com.bytedance.bytehouse.misc.BytesHelper;
import io.airlift.compress.Decompressor;
import io.airlift.compress.lz4.Lz4Decompressor;
import java.io.IOException;

import static com.bytedance.bytehouse.settings.BHConstants.CHECKSUM_LENGTH;
import static com.bytedance.bytehouse.settings.BHConstants.COMPRESSION_HEADER_LENGTH;

/**
 * {@link CompressedBuffedReader} supporting LZ4 fast compression.
 */
public class CompressedBuffedReader implements BuffedReader, BytesHelper {

    /* @formatter:off */
    private static final int NONE = 0x02;

    private static final int LZ4 = 0x82;

    private final BuffedReader buf;

    private final Decompressor lz4Decompressor = new Lz4Decompressor();

    private int position;

    private int capacity;

    private byte[] decompressed;

    /**
     * constructor.
     */
    public CompressedBuffedReader(final BuffedReader buf) {
        this.buf = buf;
    }

    @Override
    public int readBinary() throws IOException {
        if (position == capacity) {
            decompressed = readCompressedData();
            this.position = 0;
            this.capacity = decompressed.length;
        }

        return decompressed[position++];
    }

    @Override
    public int readBinary(final byte[] bytes) throws IOException {
        int i = 0;
        while (i < bytes.length) {
            if (position == capacity) {
                decompressed = readCompressedData();
                this.position = 0;
                this.capacity = decompressed.length;
            }

            final int padding = bytes.length - i;
            final int fillLength = Math.min(padding, capacity - position);

            if (fillLength > 0) {
                System.arraycopy(decompressed, position, bytes, i, fillLength);

                i += fillLength;
                this.position += fillLength;
            }
        }
        return bytes.length;
    }
    /* @formatter:on */

    private byte[] readCompressedData() throws IOException {
        //TODO: validate checksum
        buf.readBinary(new byte[CHECKSUM_LENGTH]);

        final byte[] compressedHeader = new byte[COMPRESSION_HEADER_LENGTH];

        if (buf.readBinary(compressedHeader) != COMPRESSION_HEADER_LENGTH) {
            throw new IOException("Invalid compression header");
        }

        final int method = compressedHeader[0] & 0x0FF;
        final int compressedSize = getIntLE(compressedHeader, 1);
        final int decompressedSize = getIntLE(compressedHeader, 5);

        switch (method) {
            case LZ4:
                return readLZ4CompressedData(
                        compressedSize - COMPRESSION_HEADER_LENGTH,
                        decompressedSize
                );
            case NONE:
                return readNoneCompressedData(decompressedSize);
            default:
                throw new UnsupportedOperationException("Unknown compression magic code: "
                        + method);
        }
    }

    private byte[] readNoneCompressedData(final int size) throws IOException {
        final byte[] decompressed = new byte[size];

        if (buf.readBinary(decompressed) != size) {
            throw new IOException("Cannot decompress use None method.");
        }

        return decompressed;
    }

    private byte[] readLZ4CompressedData(
            final int compressedSize,
            final int decompressedSize
    ) throws IOException {
        final byte[] compressed = new byte[compressedSize];
        if (buf.readBinary(compressed) == compressedSize) {
            final byte[] decompressed = new byte[decompressedSize];

            if (lz4Decompressor.decompress(
                    compressed,
                    0,
                    compressedSize,
                    decompressed,
                    0,
                    decompressedSize
            ) == decompressedSize) {
                return decompressed;
            }
        }

        throw new IOException("Cannot decompress use LZ4 method.");
    }
}
