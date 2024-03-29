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
package com.bytedance.bytehouse.data;

import com.bytedance.bytehouse.client.ServerContext;
import com.bytedance.bytehouse.data.type.DataTypeBitMap64;
import com.bytedance.bytehouse.data.type.DataTypeDate;
import com.bytedance.bytehouse.data.type.DataTypeFloat32;
import com.bytedance.bytehouse.data.type.DataTypeFloat64;
import com.bytedance.bytehouse.data.type.DataTypeIPv4;
import com.bytedance.bytehouse.data.type.DataTypeIPv6;
import com.bytedance.bytehouse.data.type.DataTypeInt16;
import com.bytedance.bytehouse.data.type.DataTypeInt32;
import com.bytedance.bytehouse.data.type.DataTypeInt64;
import com.bytedance.bytehouse.data.type.DataTypeInt8;
import com.bytedance.bytehouse.data.type.DataTypeUInt128;
import com.bytedance.bytehouse.data.type.DataTypeUInt16;
import com.bytedance.bytehouse.data.type.DataTypeUInt256;
import com.bytedance.bytehouse.data.type.DataTypeUInt32;
import com.bytedance.bytehouse.data.type.DataTypeUInt64;
import com.bytedance.bytehouse.data.type.DataTypeUInt8;
import com.bytedance.bytehouse.data.type.DataTypeUUID;
import com.bytedance.bytehouse.data.type.complex.DataTypeArray;
import com.bytedance.bytehouse.data.type.complex.DataTypeCreator;
import com.bytedance.bytehouse.data.type.complex.DataTypeDateTime;
import com.bytedance.bytehouse.data.type.complex.DataTypeDateTime64;
import com.bytedance.bytehouse.data.type.complex.DataTypeDecimal;
import com.bytedance.bytehouse.data.type.complex.DataTypeEnum16;
import com.bytedance.bytehouse.data.type.complex.DataTypeEnum8;
import com.bytedance.bytehouse.data.type.complex.DataTypeFixedString;
import com.bytedance.bytehouse.data.type.complex.DataTypeLowCardinality;
import com.bytedance.bytehouse.data.type.complex.DataTypeMap;
import com.bytedance.bytehouse.data.type.complex.DataTypeNothing;
import com.bytedance.bytehouse.data.type.complex.DataTypeNullable;
import com.bytedance.bytehouse.data.type.complex.DataTypeString;
import com.bytedance.bytehouse.data.type.complex.DataTypeTuple;
import com.bytedance.bytehouse.misc.LRUCache;
import com.bytedance.bytehouse.misc.SQLLexer;
import com.bytedance.bytehouse.misc.ValidateUtils;
import com.bytedance.bytehouse.settings.BHConstants;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Methods to generate {@link IDataType}.
 */
public final class DataTypeFactory {

    private static final LRUCache<String, IDataType<?, ?>> DATA_TYPE_CACHE =
            new LRUCache<>(BHConstants.DATA_TYPE_CACHE_SIZE);

    private static final Map<String, IDataType<?, ?>> DATA_TYPES = initialDataTypes();

    private DataTypeFactory() {
        // no creation
    }

    /**
     * Parsing a subclass of {@link IDataType} from the string.
     */
    public static IDataType<?, ?> get(
            final String type,
            final ServerContext serverContext
    ) throws SQLException {
        IDataType<?, ?> dataType = DATA_TYPE_CACHE.get(type);
        if (dataType != null) {
            DATA_TYPE_CACHE.put(type, dataType);
            return dataType;
        }

        final SQLLexer lexer = new SQLLexer(0, type);
        dataType = get(lexer, serverContext);
        ValidateUtils.isTrue(lexer.eof());

        DATA_TYPE_CACHE.put(type, dataType);
        return dataType;
    }

    /**
     * parse {@link IDataType} from {@link SQLLexer}.
     */
    public static IDataType<?, ?> get(
            final SQLLexer lexer,
            final ServerContext serverContext
    ) throws SQLException {
        final String dataTypeName = String.valueOf(lexer.bareWord());

        if (dataTypeName.equalsIgnoreCase("Tuple")) {
            return DataTypeTuple.creator.createDataType(lexer, serverContext);
        } else if (dataTypeName.equalsIgnoreCase("Array")) {
            return DataTypeArray.creator.createDataType(lexer, serverContext);
        } else if (dataTypeName.equalsIgnoreCase("Enum8")) {
            return DataTypeEnum8.creator.createDataType(lexer, serverContext);
        } else if (dataTypeName.equalsIgnoreCase("Enum16")) {
            return DataTypeEnum16.creator.createDataType(lexer, serverContext);
        } else if (dataTypeName.equalsIgnoreCase("DateTime")) {
            return DataTypeDateTime.creator.createDataType(lexer, serverContext);
        } else if (dataTypeName.equalsIgnoreCase("DateTime64")) {
            return DataTypeDateTime64.creator.createDataType(lexer, serverContext);
        } else if (dataTypeName.equalsIgnoreCase("Nullable")) {
            return DataTypeNullable.creator.createDataType(lexer, serverContext);
        } else if (dataTypeName.equalsIgnoreCase("FixedString") || dataTypeName.equals("Binary")) {
            return DataTypeFixedString.creator.createDataType(lexer, serverContext);
        } else if (dataTypeName.equalsIgnoreCase("Decimal")) {
            return DataTypeDecimal.creator.createDataType(lexer, serverContext);
        } else if (dataTypeName.equalsIgnoreCase("String")) {
            return DataTypeString.CREATOR.createDataType(lexer, serverContext);
        } else if (dataTypeName.equalsIgnoreCase("Nothing")) {
            return DataTypeNothing.CREATOR.createDataType(lexer, serverContext);
        } else if (dataTypeName.equalsIgnoreCase("LowCardinality")) {
            return DataTypeLowCardinality.creator.createDataType(lexer, serverContext);
        } else if (dataTypeName.equalsIgnoreCase("Map")) {
            return DataTypeMap.creator.createDataType(lexer, serverContext);
        } else {
            IDataType<?, ?> dataType = DATA_TYPES.get(dataTypeName.toLowerCase(Locale.ROOT));
            ValidateUtils.isTrue(dataType != null, "Unknown data type: " + dataTypeName);
            return dataType;
        }
    }

    /**
     * Some framework like Spark JDBC will convert all types name to lower case
     */
    private static Map<String, IDataType<?, ?>> initialDataTypes() {
        Map<String, IDataType<?, ?>> creators = new HashMap<>();

        registerType(creators, new DataTypeIPv4());
        registerType(creators, new DataTypeIPv6());
        registerType(creators, new DataTypeUUID());
        registerType(creators, new DataTypeBitMap64());
        registerType(creators, new DataTypeFloat32());
        registerType(creators, new DataTypeFloat64());

        registerType(creators, new DataTypeInt8());
        registerType(creators, new DataTypeInt16());
        registerType(creators, new DataTypeInt32());
        registerType(creators, new DataTypeInt64());

        registerType(creators, new DataTypeUInt8());
        registerType(creators, new DataTypeUInt16());
        registerType(creators, new DataTypeUInt32());
        registerType(creators, new DataTypeUInt64());
        registerType(creators, new DataTypeUInt128());
        registerType(creators, new DataTypeUInt256());

        registerType(creators, new DataTypeDate());
        return creators;
    }

    private static void registerType(
            final Map<String, IDataType<?, ?>> creators,
            final IDataType<?, ?> type
    ) {
        creators.put(type.name().toLowerCase(Locale.ROOT), type);
        for (final String typeName : type.getAliases()) {
            creators.put(typeName.toLowerCase(Locale.ROOT), type);
        }
    }

    // TODO: ?? legacy comment from the open source
    private static Map<String, DataTypeCreator<?, ?>> initComplexDataTypes() {
        return new HashMap<>();
    }

    private static void registerComplexType(
            final Map<String, DataTypeCreator<?, ?>> creators,
            final IDataType<?, ?> type,
            final DataTypeCreator<?, ?> creator
    ) {

        creators.put(type.name().toLowerCase(Locale.ROOT), creator);
        for (String typeName : type.getAliases()) {
            creators.put(typeName.toLowerCase(Locale.ROOT), creator);
        }
    }
}
