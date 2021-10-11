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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class ResultWriter {
    private static List<String> queryTypes = new ArrayList<>();
    private static List<String> dataTypes = new ArrayList<>();
    private static List<Double> columnUnitSizes = new ArrayList<>();
    private static List<Double> columnCounts = new ArrayList<>();
    private static List<Double> rowSizes = new ArrayList<>();
    private static List<Double> rowCounts = new ArrayList<>();
    private static List<Double> totalSizes = new ArrayList<>();
    private static List<Double> timesInSeconds = new ArrayList<>();
    private static List<Double> thoroughputs = new ArrayList<>();

    private static String inputFilePath = "benchmark/reports/benchmark-datatypes.txt";
    private static String outputFilePath = "benchmark/reports/benchmark-datatypes-1.csv";

    private static int rowCount = 0;
    private static int columnCount = 9;

    public static void write() {
        loadData();
        writeCSV();
    }

    public static void loadData() {
        try {
            File inputFile = new File(inputFilePath);
            Scanner reader = new Scanner(inputFile);
            reader.nextLine();
            while (reader.hasNextLine()) {
                rowCount++;
                String line = reader.nextLine();
                String[] data = line.split("\\s+");
                if (data[0].contains("BatchInsertIBenchmark")) {
                    queryTypes.add("BATCH INSERT");
                } else if (data[0].contains("SelectIBenchmark")) {
                    queryTypes.add("SELECT");
                } else {
                    queryTypes.add("UNKNOWN");
                }

                dataTypes.add(data[3]);

                Type type = Type.from(data[3]);
                Double columnUnitSize = Double.valueOf(type.size());
                columnUnitSizes.add(truncateDouble(columnUnitSize));

                Double columnCount = Double.valueOf(data[2]);
                columnCounts.add(truncateDouble(columnCount));

                Double rowSize = columnUnitSize * columnCount;
                rowSizes.add(truncateDouble(rowSize));

                Double rowCount = Double.valueOf(data[1]);
                rowCounts.add(truncateDouble(rowCount));

                Double totalSize = rowCount*rowSize;
                totalSizes.add(truncateDouble(totalSize));

                Double timesInSecond = Double.valueOf(data[5]);
                timesInSeconds.add(truncateDouble(timesInSecond));

                Double thoroughput = totalSize / timesInSecond;
                thoroughputs.add(truncateDouble(thoroughput));
            }
            reader.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static Double truncateDouble(Double x) {
        return BigDecimal.valueOf(x)
                .setScale(2, RoundingMode.HALF_UP)
                .doubleValue();
    }

    public static void writeCSV() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder = writeHeader(stringBuilder);
        for (int i=0; i<rowCount; i++) {
            String[] data = new String[columnCount];
            data[0] = queryTypes.get(i);
            data[1] = dataTypes.get(i);
            data[2] = String.valueOf(columnUnitSizes.get(i));
            data[3] = String.valueOf(columnCounts.get(i));
            data[4] = String.valueOf(rowSizes.get(i));
            data[5] = String.valueOf(rowCounts.get(i));
            data[6] = String.valueOf(totalSizes.get(i));
            data[7] = String.valueOf(timesInSeconds.get(i));
            data[8] = String.valueOf(thoroughputs.get(i));
            stringBuilder = writeLine(stringBuilder, data);
        }

        try (PrintWriter writer = new PrintWriter(outputFilePath)) {
            writer.write(stringBuilder.toString());
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static StringBuilder writeHeader(StringBuilder stringBuilder) {
        stringBuilder.append("Query Type,");
        stringBuilder.append("Data Type,");
        stringBuilder.append("Unit Column Size,");
        stringBuilder.append("Column Count,");
        stringBuilder.append("Row Size,");
        stringBuilder.append("Batch Size,");
        stringBuilder.append("Total Size,");
        stringBuilder.append("Time,");
        stringBuilder.append("Throughput\n");
        return stringBuilder;
    }

    public static StringBuilder writeLine(StringBuilder stringBuilder, String[] data) {
        for (int i=0; i< data.length; i++) {
            stringBuilder.append(data[i]);
            if (i == data.length-1) {
                stringBuilder.append('\n');
            } else {
                stringBuilder.append(',');
            }
        }
        return stringBuilder;
    }
}
