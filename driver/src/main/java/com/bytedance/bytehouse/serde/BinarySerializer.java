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
package com.bytedance.bytehouse.serde;

import com.bytedance.bytehouse.buffer.BuffedWriter;
import com.bytedance.bytehouse.buffer.CompressedBuffedWriter;
import com.bytedance.bytehouse.misc.Switcher;
import com.bytedance.bytehouse.settings.BHConstants;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * Serializer JVM objects into bytes and stored them int {@link java.nio.ByteBuffer}.
 */
@SuppressWarnings("PMD.AvoidReassigningParameters")
public class BinarySerializer {

    private final Switcher<BuffedWriter> switcher;

    private volatile boolean enableCompression;

    public BinarySerializer(
            final BuffedWriter writer,
            final boolean enableCompression) {
        this.enableCompression = enableCompression;
        final BuffedWriter compressWriter = new CompressedBuffedWriter(
                BHConstants.SOCKET_SEND_BUFFER_BYTES,
                writer
        );
        switcher = new Switcher<>(compressWriter, writer);
    }

    public void setEnableCompression(final boolean enableCompression) {
        this.enableCompression = enableCompression;
    }

    public void writeVarInt(long x) throws IOException {
        for (int i = 0; i < 9; i++) {
            byte byt = (byte) (x & 0x7F);

            if (x > 0x7F) {
                byt |= 0x80;
            }

            x >>= 7;
            switcher.get().writeBinary(byt);

            if (x == 0) {
                return;
            }
        }
    }

    public void writeByte(final byte x) throws IOException {
        switcher.get().writeBinary(x);
    }

    public void writeBoolean(final boolean x) throws IOException {
        writeVarInt((byte) (x ? 1 : 0));
    }

    @SuppressWarnings("PointlessBitwiseExpression")
    public void writeShort(final short i) throws IOException {
        // @formatter:off
        switcher.get().writeBinary((byte) ((i >> 0) & 0xFF));
        switcher.get().writeBinary((byte) ((i >> 8) & 0xFF));
        // @formatter:on
    }

    @SuppressWarnings("PointlessBitwiseExpression")
    public void writeInt(final int i) throws IOException {
        // @formatter:off
        switcher.get().writeBinary((byte) ((i >> 0) & 0xFF));
        switcher.get().writeBinary((byte) ((i >> 8) & 0xFF));
        switcher.get().writeBinary((byte) ((i >> 16) & 0xFF));
        switcher.get().writeBinary((byte) ((i >> 24) & 0xFF));
        // @formatter:on
    }

    @SuppressWarnings("PointlessBitwiseExpression")
    public void writeLong(final long i) throws IOException {
        // @formatter:off
        switcher.get().writeBinary((byte) ((i >> 0) & 0xFF));
        switcher.get().writeBinary((byte) ((i >> 8) & 0xFF));
        switcher.get().writeBinary((byte) ((i >> 16) & 0xFF));
        switcher.get().writeBinary((byte) ((i >> 24) & 0xFF));
        switcher.get().writeBinary((byte) ((i >> 32) & 0xFF));
        switcher.get().writeBinary((byte) ((i >> 40) & 0xFF));
        switcher.get().writeBinary((byte) ((i >> 48) & 0xFF));
        switcher.get().writeBinary((byte) ((i >> 56) & 0xFF));
        // @formatter:on
    }

    public void writeUTF8StringBinary(final String utf8) throws IOException {
        writeStringBinary(utf8, StandardCharsets.UTF_8);
    }

    public void writeStringBinary(final String data, Charset charset) throws IOException {
        byte[] bs = data.getBytes(charset);
        writeBytesBinary(bs);
    }

    public void writeBytesBinary(final byte[] bs) throws IOException {
        writeVarInt(bs.length);
        switcher.get().writeBinary(bs);
    }

    public void flushToTarget(final boolean force) throws IOException {
        switcher.get().flushToTarget(force);
    }

    /**
     * enable compression if default is on.
     */
    public void maybeEnableCompressed() {
        if (enableCompression) {
            switcher.select(false);
        }
    }

    /**
     * disable compression if default is off.
     */
    public void maybeDisableCompressed() throws IOException {
        if (enableCompression) {
            switcher.get().flushToTarget(true);
            switcher.select(true);
        }
    }

    public void writeFloat(final float datum) throws IOException {
        int x = Float.floatToIntBits(datum);
        writeInt(x);
    }

    @SuppressWarnings("PointlessBitwiseExpression")
    public void writeDouble(final double datum) throws IOException {
        long x = Double.doubleToLongBits(datum);
        // @formatter:off
        switcher.get().writeBinary((byte) ((x >>> 0) & 0xFF));
        switcher.get().writeBinary((byte) ((x >>> 8) & 0xFF));
        switcher.get().writeBinary((byte) ((x >>> 16) & 0xFF));
        switcher.get().writeBinary((byte) ((x >>> 24) & 0xFF));
        switcher.get().writeBinary((byte) ((x >>> 32) & 0xFF));
        switcher.get().writeBinary((byte) ((x >>> 40) & 0xFF));
        switcher.get().writeBinary((byte) ((x >>> 48) & 0xFF));
        switcher.get().writeBinary((byte) ((x >>> 56) & 0xFF));
        // @formatter:on
    }

    public void writeBytes(final byte[] bytes) throws IOException {
        switcher.get().writeBinary(bytes);
    }
}
