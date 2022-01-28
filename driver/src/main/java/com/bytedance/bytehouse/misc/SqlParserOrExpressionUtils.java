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

public class SqlParserOrExpressionUtils {
    private static final String OR = " OR ";

    public static String resolve(String expression) {
        if (expression.contains(OR)) {
            return engine(expression);
        }
        else {
            return expression;
        }
    }

    public static String engine(String expression) {
        int leftPtr = -1, rightPtr = -1;
        for (int i=0; i<expression.length(); i++) {
            StringBuilder tempString = new StringBuilder();
            for (int j=0; j<OR.length(); j++) {
                tempString.append(expression.charAt(i+j));
            }
            if (tempString.toString().equals(OR)) {
                leftPtr = i-1;
                rightPtr = i + OR.length();
                break;
            }
        }
        StringBuilder leftArg =  new StringBuilder();
        while (leftPtr >= 0 && expression.charAt(leftPtr) != ' ' && expression.charAt(leftPtr) != '(') {
            if (expression.charAt(leftPtr) == ')') {
                return expression;
            }
            leftArg.append(expression.charAt(leftPtr--));
        }
        if (expression.charAt(leftPtr) != '(') {
            return expression;
        }
        String arg1 = leftArg.reverse().toString();

        StringBuilder rightArg =  new StringBuilder();
        while (rightPtr < expression.length() && expression.charAt(rightPtr) != ' ' && expression.charAt(rightPtr) != ')') {
            if (expression.charAt(rightPtr) == '(') {
                return expression;
            }
            rightArg.append(expression.charAt(rightPtr++));
        }
        if (expression.charAt(rightPtr) != ')') {
            return expression;
        }
        String arg2 = rightArg.toString();
        String resolved = buildExpression(arg1, arg2);
        leftPtr++;
        rightPtr--;
        String generated = replace(expression, leftPtr, rightPtr, resolved);
        return generated;
    }

    public static String buildExpression(String arg1, String arg2) {
        return String.format("CASE WHEN isNull(%s) AND isNull(%s) THEN NULL ELSE %s OR %s END", arg1, arg2, arg1, arg2);
    }

    public static String replace(String expression, int leftPtr, int rightPtr, String resolved) {
        StringBuilder stringBuilder = new StringBuilder();
        for (int i=0; i<expression.length(); i++) {
            if (i < leftPtr || i > rightPtr) {
                stringBuilder.append(expression.charAt(i));
            }
            else if (i == leftPtr) {
                stringBuilder.append(resolved);
            }
        }
        return stringBuilder.toString();
    }

    public static void main(String[] args) {
        String query = "\n"
                + "      SELECT (\"calcs\".\"bool0\" OR \"calcs\".\"bool1\") AS \"TEMP(Test)(4182992858)(0)\"\n"
                + "FROM \"testv1\".\"calcs\" \"calcs\"\n"
                + "GROUP BY \"TEMP(Test)(4182992858)(0)\"\n"
                + "    ";
        System.out.println(resolve(query));
    }
}
