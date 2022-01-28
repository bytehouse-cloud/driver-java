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

public final class SqlParserDateFormatUtils {

    private static final String DATE_EXPRESSION = "TABLEAU.DATE";
    private static final String TO_DATE = "toDate";

    private SqlParserDateFormatUtils() {
        // Private constructor, call resolve() method
    }

    public static String resolve(String expression) {
        String expressionHolder = expression;
        if (isResolvable(expressionHolder)) {
            expressionHolder = engine(expressionHolder);
        } else {
            String generated = expressionHolder.replace(DATE_EXPRESSION, "");
            return generated;
        }
        return expressionHolder;
    }

    private static boolean isResolvable(String expression) {
        return expression.contains(DATE_EXPRESSION + "(toDate(") && expression.contains("yyyy-dd-MM");
    }

    private static String engine(String expression) {
        int leftWeekExpression = -1, rightWeekExpression = -1;
        for (int i=0; i<expression.length(); i++) {
            StringBuilder tempBuilder = new StringBuilder();
            for (int j=i; j<i+DATE_EXPRESSION.length(); j++) {
                tempBuilder.append(expression.charAt(j));
            }
            if (tempBuilder.toString().equals(DATE_EXPRESSION)) {
                leftWeekExpression = i+DATE_EXPRESSION.length()+1+TO_DATE.length()+1+1;
                break;
            }
        }
        rightWeekExpression += leftWeekExpression + 11;
        StringBuilder date = new StringBuilder();
        for (int i=leftWeekExpression; i<rightWeekExpression; i++) {
            date.append(expression.charAt(i));
        }
        String replaced = replace(expression, changeDate(date.toString()), leftWeekExpression, rightWeekExpression);
        String deletedKeyword = replaced.replace(DATE_EXPRESSION, "");
        return deletedKeyword;
    }

    private static String replace(String sql, String replaced, int leftPtr, int rightPtr) {
        StringBuilder stringBuilder = new StringBuilder();
        for (int i=0; i<sql.length(); i++) {
            if (i < leftPtr || i >= rightPtr) {
                stringBuilder.append(sql.charAt(i));
            }
            if (i == leftPtr) {
                stringBuilder.append(replaced);
            }
        }
        return stringBuilder.toString();
    }

    private static String changeDate(String date) {
        String[] strings = date.split("-");
        return strings[0] + "-" + strings[2] + "-" + strings[1];
    }
}
