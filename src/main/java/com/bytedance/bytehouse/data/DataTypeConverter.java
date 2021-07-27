package com.bytedance.bytehouse.data;

import com.bytedance.bytehouse.data.type.*;
import com.bytedance.bytehouse.data.type.complex.*;
import com.bytedance.bytehouse.exception.ByteHouseSQLException;
import com.bytedance.bytehouse.jdbc.ByteHouseArray;
import com.bytedance.bytehouse.jdbc.ByteHouseStruct;
import com.bytedance.bytehouse.log.Logger;
import com.bytedance.bytehouse.log.LoggerFactory;

import java.time.ZoneId;

import static com.bytedance.bytehouse.misc.ExceptionUtil.unchecked;

/**
 * Converter layer between internal representation of ByteHouse data type (Java type) and JDBC types.
 */
public class DataTypeConverter {

    private static final Logger LOG = LoggerFactory.getLogger(DataTypeConverter.class);

    private final ZoneId tz;

    public DataTypeConverter(ZoneId tz) {
        this.tz = tz;
    }

    /**
     * Converts JDBC type to Java type.
     */
    public Object convertJdbcToJava(IDataType<?, ?> type, Object obj) throws ByteHouseSQLException {
        if (obj == null) {
            if (type.nullable() || type instanceof DataTypeNothing)
                return null;
            throw new ByteHouseSQLException(-1, "type[" + type.name() + "] doesn't support null value");
        }
        // handle special types
        if (type instanceof DataTypeNullable) {
            // handled null at first, so obj also not null here
            return convertJdbcToJava(((DataTypeNullable) type).getNestedDataType(), obj);
        }
        if (type instanceof DataTypeArray) {
            if (!(obj instanceof ByteHouseArray)) {
                throw new ByteHouseSQLException(-1, "require ByteHouseArray for column: " + type.name() + ", but found " + obj.getClass());
            }
            return ((ByteHouseArray) obj).mapElements(unchecked(this::convertJdbcToJava));
        }
        if (type instanceof DataTypeBitMap64) {
            if (!(obj instanceof ByteHouseArray)) {
                throw new ByteHouseSQLException(-1, "require ByteHouseArray for column: " + type.name() + ", but found " + obj.getClass());
            }
            return ((ByteHouseArray) obj).mapElements(unchecked(this::convertJdbcToJava));
        }
        if (type instanceof DataTypeTuple) {
            if (!(obj instanceof ByteHouseStruct)) {
                throw new ByteHouseSQLException(-1, "require ByteHouseStruct for column: " + type.name() + ", but found " + obj.getClass());
            }
            return ((ByteHouseStruct) obj).mapAttributes(((DataTypeTuple) type).getNestedTypes(), unchecked(this::convertJdbcToJava));
        }
        // convert jdbc to java type
        return type.convertJdbcToJavaType(obj, tz);
    }
}
