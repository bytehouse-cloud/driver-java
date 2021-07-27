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

package com.bytedance.bytehouse.data;

import com.bytedance.bytehouse.exception.ByteHouseSQLException;
import com.bytedance.bytehouse.exception.NoDefaultValueException;
import com.bytedance.bytehouse.exception.NotImplementedException;
import com.bytedance.bytehouse.misc.SQLLexer;
import com.bytedance.bytehouse.serde.BinaryDeserializer;
import com.bytedance.bytehouse.serde.BinarySerializer;

import java.io.IOException;
import java.sql.SQLException;
import java.time.ZoneId;

/**
 * Interface for data type implementations. All ByteHouse data types will implement this interface.
 *
 * @param <CK> Java class the ByteHouse data type will be internally represented as.
 * @param <JDBC> Main JDBC type the CK Java class will be converted to and fro.
 */
public interface IDataType<CK, JDBC> {

    /**
     * Returns ByteHouse data type name.
     * @return name
     */
    String name();

    /**
     * Returns alternative names.
     * @return array of alternative names
     */
    default String[] getAliases() {
        return new String[0];
    }

    /**
     * Returns defaultValue.
     * @return defaultValue in CK type.
     */
    default CK defaultValue() {
        throw new NoDefaultValueException("Column[" + name() + "] doesn't has default value");
    }

    /**
     * Returns class of CK type.
     * @return class of CK type
     */
    Class<CK> javaType();

    /**
     * Returns int of JDBC type.
     * @return one of the values in java.sql.Types
     */
    int sqlTypeId();

    /**
     * Returns class of JDBC type.
     * @return class of JDBC type
     */
    @SuppressWarnings("unchecked")
    default Class<JDBC> jdbcJavaType() {
        return (Class<JDBC>) javaType();
    }

    /**
     * Returns whether this data type is nullable.
     * @return boolean
     */
    default boolean nullable() {
        return false;
    }

    /**
     * Returns whether this data type is signed.
     * @return boolean
     */
    default boolean isSigned() {
        return false;
    }

    /**
     * Returns precision of data type. 0 is returned when precision is not applicable.
     * @return int
     */
    int getPrecision();

    /**
     * Returns scale of data type. 0 is returned when scale is not applicable.
     * @return int
     */
    int getScale();

    /**
     * Converts CK value to String.
     * @param value value in CK type.
     * @return CK value as String
     */
    default String serializeText(CK value) {
        return value.toString();
    }

    /**
     * Serializes data to format for transfer to server.
     * @param data cell data
     * @param serializer serializer
     * @throws SQLException general exception
     * @throws IOException exception when serializing
     */
    void serializeBinary(CK data, BinarySerializer serializer) throws SQLException, IOException;

    /**
     * Serializes data for entire column to format for transfer to server.
     * @param data cell data
     * @param serializer serializer
     * @throws SQLException general exception
     * @throws IOException exception when serializing
     */
    default void serializeBinaryBulk(CK[] data, BinarySerializer serializer) throws SQLException, IOException {
        for (CK d : data) {
            serializeBinary(d, serializer);
        }
    }

    /**
     * Converts text in lexer to CK type object.
     * @param lexer lexer
     * @throws SQLException general exception
     * @return CK type object
     */
    CK deserializeText(SQLLexer lexer) throws SQLException;

    /**
     * Deserializes data from server to CK type.
     * @param deserializer deserializer
     * @throws SQLException general exception
     * @throws IOException exception when serializing
     * @return CK type object
     */
    CK deserializeBinary(BinaryDeserializer deserializer) throws SQLException, IOException;

    /**
     * Deserializes data from server (for entire column) to CK type.
     * @param rows number of rows in column
     * @param deserializer deserializer
     * @throws SQLException general exception
     * @throws IOException exception when serializing
     * @return array of CK type objects
     */
    default Object[] deserializeBinaryBulk(int rows, BinaryDeserializer deserializer) throws SQLException, IOException {
        Object[] data = new Object[rows];
        for (int row = 0; row < rows; row++) {
            data[row] = this.deserializeBinary(deserializer);
        }
        return data;
    }

    /**
     * Converts obj of JDBC type to CK java type.
     * @param obj jdbc type
     * @param tz zone id from server
     * @return CK java type
     * @throws ByteHouseSQLException when conversion fails
     */
    default CK convertJdbcToJavaType(Object obj, ZoneId tz) throws ByteHouseSQLException {
        throw new NotImplementedException("method not implemented.");
    }
}
