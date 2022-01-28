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
package com.bytedance.bytehouse.misc;

public final class SqlParserCaseExpressionUtils {
    private static final String CASE = "CASE";
    private static final String WHEN = "WHEN";
    private static final String THEN = "THEN";
    private static final String ELSE = "ELSE";
    private static final String END = "END";

    private static final String WEEK_EXPRESSION = "TABLEAU.WEEK";

    private SqlParserCaseExpressionUtils() {
        // Private constructor, call resolve() method
    }

    public static String resolve(String expression) {
        String expressionHolder = expression;
        while (isResolvable(expressionHolder)) {
            expressionHolder = engine(expressionHolder);
        }
        return expressionHolder;
    }

    private static boolean isResolvable(String expression) {
        return expression.contains(WEEK_EXPRESSION);
    }

    private static String engine(String expression) {
        int leftWeekExpression = -1, rightWeekExpression = -1;
        for (int i=0; i<expression.length(); i++) {
            StringBuilder tempBuilder = new StringBuilder();
            for (int j=i; j<i+WEEK_EXPRESSION.length(); j++) {
                tempBuilder.append(expression.charAt(j));
            }
            if (tempBuilder.toString().equals(WEEK_EXPRESSION)) {
                leftWeekExpression = i+WEEK_EXPRESSION.length()+1;
                break;
            }
        }
        for (int i=leftWeekExpression; i<expression.length(); i++) {
            if (expression.charAt(i) == ')') {
                rightWeekExpression = i-1;
                break;
            }
        }
        int value = getResolvedValue(expression, leftWeekExpression);
        return replace(expression, value, leftWeekExpression-1- WEEK_EXPRESSION.length(), rightWeekExpression+1);
    }

    private static String replace(String sql, int value, int leftPtr, int rightPtr) {
        StringBuilder stringBuilder = new StringBuilder();
        for (int i=0; i<sql.length(); i++) {
            if (i < leftPtr || i > rightPtr) {
                stringBuilder.append(sql.charAt(i));
            }
            if (i == leftPtr) {
                stringBuilder.append(value);
            }
        }
        return stringBuilder.toString();
    }

    private static int getResolvedValue(String sql, int ptr) {
        ptr += CASE.length(); // Read 'CASE'
        ptr += 1; // Read space

        while (true) {
            if (sql.charAt(ptr) == 'W') { // Starts WHEN block
                ptr += WHEN.length(); // Read 'WHEN'
                ptr += 1; // Read space
                int booleanLeft = Integer.valueOf(sql.charAt(ptr++)) - Integer.valueOf('0');
                ptr += 1; // Read '='
                int booleanRight = Integer.valueOf(sql.charAt(ptr++)) - Integer.valueOf('0');
                ptr += 1; // Read space
                ptr += THEN.length(); // Read 'THEN'
                ptr += 1; // Read space
                int returnValue = Integer.valueOf(sql.charAt(ptr++)) - Integer.valueOf('0');
                ptr += 1; // Read space
                if (booleanLeft == booleanRight) {
                    return returnValue;
                }
            }
            else {
                ptr += ELSE.length(); // Read 'ELSE'
                ptr += 1; // Read '='
                int returnValue = Integer.valueOf(sql.charAt(ptr++)) - Integer.valueOf('0');
                return returnValue;
            }
        }
    }
}
