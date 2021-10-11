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
package com.bytedance.bytehouse;

@SuppressWarnings("PMD.FieldNamingConventions")
public enum Type {
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    String,
    FixedString,
    UUID,
    Float32,
    Float64,
    Decimal,
    Date,
    DateTime,
    Enum8,
    Enum16,
    Map,
    Array,
    IPv4,
    IPv6,
    Tuple;

    public String toString() {
        switch(this) {
            case Int8:
                return "Int8";
            case Int16:
                return "Int16";
            case Int32:
                return "Int32";
            case Int64:
                return "Int64";
            case UInt8:
                return "UInt8";
            case UInt16:
                return "UInt16";
            case UInt32:
                return "UInt32";
            case UInt64:
                return "UInt64";
            case String:
                return "String";
            case FixedString:
                return "FixedString(10)";
            case UUID:
                return "UUID";
            case Float32:
                return "Float32";
            case Float64:
                return "Float64";
            case Decimal:
                return "Decimal(18, 5)";
            case Date:
                return "Date";
            case DateTime:
                return "DateTime";
            case IPv4:
                return "IPv4";
            case IPv6:
                return "IPv6";
            case Array:
                return "Array(Int)";
            case Map:
                return "Map(Int32, Int32)";
            case Enum8:
                return "Enum8('hello' = 1, 'world' = 2)";
            case Tuple:
                return "Tuple(String, Int)";
            default:
                return "";
        }
    }

    public int size() {
        switch (this) {
            case Int8:
                return 1;
            case Int16:
                return 2;
            case Int32:
                return 4;
            case Int64:
                return 8;
            case UInt8:
                return 1;
            case UInt16:
                return 2;
            case UInt32:
                return 4;
            case UInt64:
                return 8;
            case String:
                return 20;
            case FixedString:
                return 10;
            case UUID:
                return 8;
            case Float32:
                return 4;
            case Float64:
                return 8;
            case Decimal:
                return 1;
            case Date:
                return 2;
            case DateTime:
                return 4;
            case IPv4:
                return 4;
            case IPv6:
                return 16;
            default:
                return 1;
        }
    }

    public static Type from(String type) {
        switch(type) {
            case "Int8":
                return Int8;
            case "Int16":
                return Int16;
            case "Int32":
                return Int32;
            case "Int64":
                return Int64;
            case "UInt8":
                return UInt8;
            case "UInt16":
                return UInt16;
            case "UInt32":
                return UInt32;
            case "UInt64":
                return UInt64;
            case "String":
                return String;
            case "FixedString":
                return FixedString;
            case "UUID":
                return UUID;
            case "Float32":
                return Float32;
            case "Float64":
                return Float64;
            case "Decimal":
                return Decimal;
            case "Date":
                return Date;
            case "DateTime":
                return DateTime;
            case "IPv4":
                return IPv4;
            case "IPv6":
                return IPv6;
            case "Array":
                return Array;
            case "Map":
                return Map;
            case "Enum8":
                return Enum8;
            case "Tuple":
                return Tuple;
            default:
                return null;
        }
    }
}
