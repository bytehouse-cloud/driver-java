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

import jdk.nashorn.internal.ir.annotations.Immutable;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import java.io.FileReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class InfileCSVReaderUtils {
    private InfileCSVReaderUtils() {
    }

    public static CSVBlock fromCSV(String fileLocation, String csvDelimiter, boolean hasHeader) throws Exception {
        if (csvDelimiter.length() == 0) {
            throw new IllegalArgumentException("invalid csv delimiter");
        }
        List<List<String>> rows = new ArrayList<>();
        List<String> headers = new ArrayList<>();

        int colSize = -1;
        try (Reader in = new FileReader(fileLocation)) {
            Iterator<CSVRecord> records = CSVFormat.DEFAULT
                .withDelimiter(csvDelimiter.charAt(csvDelimiter.length()-1))
                .parse(in)
                .iterator();
            if (records.hasNext() && hasHeader) {
                records.next().iterator().forEachRemaining(headers::add);
                colSize = headers.size();
            }
            while (records.hasNext()) {
                List<String> record = new ArrayList<>();
                records.next().iterator().forEachRemaining(record::add);
                rows.add(record);

                if (colSize == -1) {
                    colSize = record.size();
                }
                if (record.size() != colSize) {
                    throw new IllegalArgumentException("invalid csv format");
                }
            }
        }

        if ((hasHeader && headers.size() == 0) || rows.size() == 0 || rows.get(0).size() == 0) {
            throw new IllegalArgumentException("invalid csv format");
        }
        return new CSVBlock(headers, rows);
    }

    @Immutable
    public static class CSVBlock {
        public List<String> headers;
        public List<List<String>> rows;

        public CSVBlock(List<String> headers, List<List<String>> rows) {
            this.headers = headers;
            this.rows = rows;
        }

        public int getRowCount() {
            return rows.size();
        }

        public int getColumnCount() {
            return rows.get(0).size();
        }

        public List<List<String>> getRows() {
            return this.rows;
        }

        public List<String> getHeaders() {
            return this.headers;
        }
    }
}
