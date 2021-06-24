package com.bytedance.bytehouse.protocol;

import com.bytedance.bytehouse.serde.BinaryDeserializer;

import java.io.IOException;
import java.sql.SQLException;

public class TableColumnsResponse implements Response {

    /**
     * readFrom implementation follows
     * <a href="https://code.byted.org/bytehouse/driver-go/blob/main/driver/response/table_columns.go">
     *     table_columns.go
     * </a>
     */
    public static TableColumnsResponse readFrom(BinaryDeserializer deserializer)
            throws IOException, SQLException {

        return new TableColumnsResponse(
                deserializer.readUTF8StringBinary(),
                deserializer.readUTF8StringBinary()
        );
    }

    private final String name;

    private final String description;

    public TableColumnsResponse(String name, String description) {
        this.name = name;
        this.description = description;
    }

    @Override
    public ProtoType type() {
        return ProtoType.RESPONSE_TABLE_COLUMNS;
    }

    public String name() {
        return name;
    }

    public String description() {
        return description;
    }
}
