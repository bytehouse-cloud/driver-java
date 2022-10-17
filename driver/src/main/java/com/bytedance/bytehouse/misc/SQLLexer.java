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
package com.bytedance.bytehouse.misc;

import java.math.BigInteger;
import java.sql.SQLException;

/**
 * utility tool that given a string and a starting position, it reads the characters
 * one by one and decode its content.
 */
public class SQLLexer {

    private final String data;

    private int currPos;

    private class NumberTypeRep {
        final boolean isHex;
        final boolean isBinary;
        final boolean isDouble;
        final boolean hasExponent;
        final boolean hasSigned;
        final int start;

        NumberTypeRep(
            boolean isHex, boolean isBinary,
            boolean isDouble, boolean hasExponent,
            boolean hasSigned, int start
        ) {
            this.isHex = isHex;
            this.isBinary = isBinary;
            this.isDouble = isDouble;
            this.hasExponent = hasExponent;
            this.hasSigned = hasSigned;
            this.start = start;
        }

        String getNormalStr() {
            return new StringView(data, this.start, currPos).toString();
        }

        String getSignedStr() {
            String signed = this.hasSigned ? data.charAt(this.start) + "" : "";
            int begin = this.start + (this.hasSigned ? 3 : 2);
            return signed + new StringView(data, begin, currPos);
        }
    }

    public SQLLexer(final int startingPos, final String data) {
        this.currPos = startingPos;
        this.data = data;
    }

    public char character() {
        return eof() ? 0 : data.charAt(currPos++);
    }

    // only support dec
    public int intLiteral() {
        skipAnyWhitespace();

        int start = currPos;

        if (isCharacter('-') || isCharacter('+'))
            currPos++;

        for (; currPos < data.length(); currPos++)
            if (!isNumericASCII(data.charAt(currPos)))
                break;

        return Integer.parseInt(new StringView(data, start, currPos).toString());
    }

    public BigInteger bigIntegerLiteral() {
        NumberTypeRep typeRep = preprocessGenericNumberLiteral();
        if (typeRep.isBinary) {
            return new BigInteger(typeRep.getSignedStr(), 2);
        } else if (typeRep.isHex) {
            return new BigInteger(typeRep.getSignedStr(), 16);
        } else {
            return new BigInteger(typeRep.getNormalStr());
        }
    }

    public Number numberLiteral() {
        NumberTypeRep typeRep = preprocessGenericNumberLiteral();
        if (typeRep.isBinary) {
            return Long.parseLong(typeRep.getSignedStr(), 2);
        } else if (typeRep.isDouble || typeRep.hasExponent) {
            return Double.valueOf(typeRep.getNormalStr());
        } else if (typeRep.isHex) {
            return Long.parseLong(typeRep.getSignedStr(), 16);
        } else {
            return Long.parseLong(typeRep.getNormalStr());
        }
    }

    private NumberTypeRep preprocessGenericNumberLiteral() {
        skipAnyWhitespace();

        int start = currPos;
        // @formatter:off
        boolean isHex = false;
        boolean isBinary = false;
        boolean isDouble = false;
        boolean hasExponent = false;
        boolean hasSigned = false;
        // @formatter:on

        if (isCharacter('-') || isCharacter('+')) {
            hasSigned = true;
            currPos++;
        }

        if (currPos + 2 < data.length()) {
            // @formatter:off
            if (data.charAt(currPos) == '0' && (data.charAt(currPos + 1) == 'x' || data.charAt(currPos + 1) == 'X'
                    || data.charAt(currPos + 1) == 'b' || data.charAt(currPos + 1) == 'B')) {
                isHex = data.charAt(currPos + 1) == 'x' || data.charAt(currPos + 1) == 'X';
                isBinary = data.charAt(currPos + 1) == 'b' || data.charAt(currPos + 1) == 'B';
                currPos += 2;
            }
            // @formatter:on
        }

        for (; currPos < data.length(); currPos++) {
            if (isHex ? !isHexDigit(data.charAt(currPos)) : !isNumericASCII(data.charAt(currPos))) {
                break;
            }
        }

        if (currPos < data.length() && data.charAt(currPos) == '.') {
            isDouble = true;
            for (currPos++; currPos < data.length(); currPos++) {
                if (isHex ? !isHexDigit(data.charAt(currPos)) : !isNumericASCII(data.charAt(currPos)))
                    break;
            }
        }

        if (currPos + 1 < data.length()
                // @formatter:off
                && (isHex ? (data.charAt(currPos) == 'p' || data.charAt(currPos) == 'P')
                : (data.charAt(currPos) == 'e' || data.charAt(currPos) == 'E'))) {
            // @formatter:on
            hasExponent = true;
            currPos++;

            if (currPos + 1 < data.length() && (data.charAt(currPos) == '-' || data.charAt(currPos) == '+')) {
                currPos++;
            }

            for (; currPos < data.length(); currPos++) {
                char ch = data.charAt(currPos);
                if (!isNumericASCII(ch)) {
                    break;
                }
            }
        }
        return new NumberTypeRep(isHex, isBinary, isDouble, hasExponent, hasSigned, start);
    }

    public String stringLiteral() throws SQLException {
        return stringView().toString();
    }

    public StringView stringView() throws SQLException {
        skipAnyWhitespace();
        ValidateUtils.isTrue(isCharacter('\''), "expect string to be quoted with single quote");
        return stringLiteralWithQuoted('\'');
    }

    public boolean eof() {
        skipAnyWhitespace();
        return currPos >= data.length();
    }

    public boolean isCharacter(char ch) {
        return !eof() && data.charAt(currPos) == ch;
    }

    public StringView bareWord() throws SQLException {
        skipAnyWhitespace();
        // @formatter:off
        if (isCharacter('`')) {
            return stringLiteralWithQuoted('`');
        } else if (isCharacter('"')) {
            return stringLiteralWithQuoted('"');
        } else if (data.charAt(currPos) == '_'
                || (data.charAt(currPos) >= 'a' && data.charAt(currPos) <= 'z')
                || (data.charAt(currPos) >= 'A' && data.charAt(currPos) <= 'Z')) {
            int start = currPos;
            for (currPos++; currPos < data.length(); currPos++) {
                if (!('_' == data.charAt(currPos)
                        || (data.charAt(currPos) >= 'a' && data.charAt(currPos) <= 'z')
                        || (data.charAt(currPos) >= 'A' && data.charAt(currPos) <= 'Z')
                        || (data.charAt(currPos) >= '0' && data.charAt(currPos) <= '9'))) {
                    break;
                }
            }
            return new StringView(data, start, currPos);
        }
        // @formatter:on
        throw new SQLException("Expect Bare Token.");
    }

    public boolean isWhitespace() {
        return data.charAt(currPos++) == ' ';
    }

    private boolean isNumericASCII(char c) {
        return c >= '0' && c <= '9';
    }

    private boolean isHexDigit(char c) {
        return isNumericASCII(c) || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F');
    }

    private void skipAnyWhitespace() {
        for (; currPos < data.length(); currPos++) {
            // @formatter:off
            if (data.charAt(currPos) != ' '
                    && data.charAt(currPos) != '\t'
                    && data.charAt(currPos) != '\n'
                    && data.charAt(currPos) != '\r'
                    && data.charAt(currPos) != '\f') {
                return;
            }
            // @formatter:on
        }
    }

    private StringView stringLiteralWithQuoted(char quoted) throws SQLException {
        int start = currPos;
        ValidateUtils.isTrue(data.charAt(currPos) == quoted);
        for (currPos++; currPos < data.length(); currPos++) {
            if (data.charAt(currPos) == '\\')
                currPos++;
            else if (data.charAt(currPos) == quoted)
                return new StringView(data, start + 1, currPos++);
        }
        throw new SQLException("The String Literal is no Closed.");
    }
}
