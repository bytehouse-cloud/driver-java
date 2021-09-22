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
package com.bytedance.bytehouse.jdbc.scenario;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * A util class to generate sql insert statement.
 */
public final class GenerateSqlInsertStatementUtil {

    public static final String[] columnTypes = {"String", "Int64", "UInt8", "Array(UInt32)"};

    public static final String[] columnValues = {"'rand_string'", "123456789123456789", "123", "[1, 2, 3, 4, 5]"};

    private GenerateSqlInsertStatementUtil() {
        // no creation of util class
    }

    public static void main(String[] args) throws IOException {
        String queryCreateTable = getSqlCreateTable("create-table");
        Map<String, String> columnProperties = getColumnProperties(queryCreateTable);
        String querySuffix = createValuesForSqlInsert(columnProperties);
        String query = "INSERT INTO " + "my_table" + " " + querySuffix;
        writeToSqlFile("insert-table", query);
    }

    public static void generateInsertQuery(String tableName) throws IOException {
        String queryCreateTable = getSqlCreateTable("create-table");
        Map<String, String> columnProperties = getColumnProperties(queryCreateTable);
        String querySuffix = createValuesForSqlInsert(columnProperties);
        String query = "INSERT INTO " + tableName + " " + querySuffix;
        writeToSqlFile("insert-table", query);
    }

    public static String createValuesForSqlInsert(Map<String, String> columnProperties) {
        String sqlInsertValues = "";
        boolean started = false;
        for (Map.Entry<String, String> columnProperty : columnProperties.entrySet()) {
            if (started == false) started = true;
            else sqlInsertValues += ",\n\t";
            sqlInsertValues += getValueForType(columnProperty.getValue());
        }

        String sqlInsertNames = "";
        started = false;
        for (Map.Entry<String, String> columnProperty : columnProperties.entrySet()) {
            if (started == false) started = true;
            else sqlInsertNames += ",\n\t";
            sqlInsertNames += columnProperty.getKey();
        }

        String query = "(" + sqlInsertNames + ") VALUES (" + sqlInsertValues + ")";
        return query;
    }

    public static void writeToSqlFile(
            final String filename,
            final String query
    ) throws IOException {
        final String fullPath = "src/test/resources/sql/" + filename + ".sql";
        try (FileWriter myWriter = new FileWriter(fullPath)) {
            myWriter.write(query);
        }
    }

    public static String getSqlCreateTable(String filename) throws IOException {
        final String fullPath = "src/test/resources/sql/" + filename + ".sql";
        return new String(
                Files.readAllBytes(Paths.get(fullPath)),
                StandardCharsets.UTF_8
        );
    }

    public static Map<String, String> getColumnProperties(String string) {
        int openBracketPos = -1, closeBracketPos = -1;
        int iter = 0;
        while (iter < string.length()) {
            if (string.charAt(iter) == '(') {
                openBracketPos = iter;
                break;
            }
            iter++;
        }
        iter = string.length() - 1;
        while (iter >= 0) {
            if (string.charAt(iter) == ')') {
                closeBracketPos = iter;
                break;
            }
            iter--;
        }

        String nameAndType = "";
        for (int i = openBracketPos + 1; i < closeBracketPos; i++) {
            nameAndType += string.charAt(i);
        }

        String[] splittedString = nameAndType.split("\\s+");
        splittedString = removeComma(splittedString);
        splittedString = removeExtraTails(splittedString);

        Map<String, String> hashMap = new HashMap<>();
        for (int i = 1; i < splittedString.length; i += 2) {
            hashMap.put(splittedString[i], splittedString[i + 1]);
        }
        return hashMap;
    }

    public static String[] removeComma(String[] stringArray) {
        for (int i = 0; i < stringArray.length; i++) {
            stringArray[i] = stringArray[i].replace(",", "");
        }
        return stringArray;
    }

    public static String[] removeExtraTails(String[] stringArray) {
        int validPosition = stringArray.length - 1;
        while (!isValidColumnType(stringArray[validPosition])) {
            validPosition--;
        }
        return Arrays.copyOfRange(stringArray, 0, validPosition + 1);
    }

    public static boolean isValidColumnType(String type) {
        for (String columnType : columnTypes) {
            if (columnType.equals(type)) return true;
        }
        return false;
    }

    public static String getValueForType(String type) {
        String value = "";
        for (int i = 0; i < columnTypes.length; i++) {
            if (columnTypes[i].equals(type)) {
                value = columnValues[i];
            }
        }
        return value;
    }

    public static String loadSqlStatement(final String filepath) throws IOException {
        final String fullPath = "src/test/resources/sql/" + filepath + ".sql";
        final byte[] bytes = Files.readAllBytes(Paths.get(fullPath));
        return new String(bytes, StandardCharsets.UTF_8);
    }
}


