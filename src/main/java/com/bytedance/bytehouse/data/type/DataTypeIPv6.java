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

package com.bytedance.bytehouse.data.type;

import com.bytedance.bytehouse.data.IDataType;
import com.bytedance.bytehouse.exception.ByteHouseSQLException;
import com.bytedance.bytehouse.exception.NoDefaultValueException;
import com.bytedance.bytehouse.misc.SQLLexer;
import com.bytedance.bytehouse.serde.BinaryDeserializer;
import com.bytedance.bytehouse.serde.BinarySerializer;

import java.io.IOException;
import java.net.Inet6Address;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.sql.Types;
import java.time.ZoneId;

/**
 * CNCH IPv6 data type
 */
public class DataTypeIPv6 implements IDataType<Inet6Address, String> {

    /** Length of IPv6 address in bytes */
    private static final int IPV6_BYTES_LEN = 16;

    @Override
    public String name() {
        return "IPv6";
    }

    @Override
    public int sqlTypeId() {
        return Types.VARCHAR;
    }

    /**
     * Returns the unspecified IPv6 address 0:0:0:0:0:0:0:0
     */
    @Override
    public Inet6Address defaultValue() {
        try {
            return (Inet6Address) Inet6Address.getByAddress(new byte[IPV6_BYTES_LEN]);
        } catch (UnknownHostException e) {
            // Should never happen
            throw new NoDefaultValueException("DataTypeIPv6 has no default value.");
        }
    }

    @Override
    public Class<Inet6Address> javaType() {
        return Inet6Address.class;
    }

    @Override
    public Class<String> jdbcJavaType() {
        return String.class;
    }

    @Override
    public int getPrecision() {
        return 0;
    }

    @Override
    public int getScale() {
        return 0;
    }

    /**
     * Serializes IPv6 address in network byte order (send most significant byte first)
     */
    @Override
    public void serializeBinary(Inet6Address data, BinarySerializer serializer) throws SQLException, IOException {
        byte[] ipv6InBytes = data.getAddress();
        for (int i = 0; i < IPV6_BYTES_LEN; i++) {
            serializer.writeByte(ipv6InBytes[i]);
        }
    }

    /**
     * Deserializes IPv6 address in network byte order (receive most significant byte first)
     */
    @Override
    public Inet6Address deserializeBinary(BinaryDeserializer deserializer) throws SQLException, IOException {
        byte[] ipv6InBytes = new byte[IPV6_BYTES_LEN];
        for (int i = 0; i < IPV6_BYTES_LEN; i++) {
            ipv6InBytes[i] = deserializer.readByte();
        }
        return (Inet6Address) Inet6Address.getByAddress(ipv6InBytes);
    }

    @Override
    public Inet6Address convertJdbcToJavaType(Object obj, ZoneId tz) throws ByteHouseSQLException {
        if (obj instanceof Inet6Address) {
            return (Inet6Address) obj;
        }
        if (obj instanceof String) {
            try {
                return ((Inet6Address) Inet6Address.getByName((String) obj));
            } catch (UnknownHostException | ClassCastException e) {
                throw new ByteHouseSQLException(-1, obj + " is not a valid IPv6 address");
            }
        }
        throw new ByteHouseSQLException(-1, obj.getClass() + " cannot convert to " + Inet6Address.class);
    }

    /**
     * Reads IPv6 address from SQL string
     * Warning: if address provided by user is a host name, this method will involve a network
     *  call to resolve the host (refer to InetAddress.getByName() method)
     */
    @Override
    public Inet6Address deserializeText(SQLLexer lexer) throws SQLException {
        try {
            return (Inet6Address) Inet6Address.getByName(lexer.stringLiteral());
        } catch (UnknownHostException | ClassCastException e) {
            throw new SQLException(e);
        }
    }
}
