package com.bytedance.bytehouse.protocol;

import com.bytedance.bytehouse.client.NativeContext;
import com.bytedance.bytehouse.data.Block;
import com.bytedance.bytehouse.serde.BinaryDeserializer;

import java.io.IOException;
import java.sql.SQLException;

public class LogResponse implements Response {

    /**
     * readFrom implementation follows
     * <a href="https://code.byted.org/bytehouse/driver-go/blob/main/driver/response/logs.go">logs.go</a>
     */
    public static LogResponse readFrom(
            BinaryDeserializer deserializer, NativeContext.ServerContext info) throws IOException, SQLException {

        String name = deserializer.readUTF8StringBinary();

        deserializer.maybeDisableCompressed();
        Block block = Block.readFrom(deserializer, info);

        return new LogResponse(name, block);
    }

    private final String name;

    private final Block block;

    public LogResponse(String name, Block block) {
        this.name = name;
        this.block = block;
    }

    @Override
    public ProtoType type() {
        return ProtoType.RESPONSE_LOG;
    }

    public String name() {
        return name;
    }

    public Block block() {
        return block;
    }
}
