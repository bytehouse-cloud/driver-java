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
package com.bytedance.bytehouse.tpcds;

import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvValidationException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class TpcdsBenchmarkSetupUtil {

    private TpcdsBenchmarkSetupUtil() {
    }

    public static String questionMarks(int count) {
        return String.join(", ", Collections.nCopies(count, "?"));
    }

    public static List<String[]> readDataFromCsv(String dataFile, char separator) {
        try (CSVReader reader = (new CSVReaderBuilder(Files.newBufferedReader(Paths.get(dataFile))))
                .withCSVParser(new CSVParserBuilder().withSeparator(separator).build())
                .build();) {
            List<String[]> data = new ArrayList<>();
            String[] nextRow = reader.readNext();
            while (nextRow != null) {
                data.add(nextRow);
                nextRow = reader.readNext();
            }
            return data;
        } catch (IOException | CsvValidationException e) {
            throw new RuntimeException(e);
        }
    }
}
