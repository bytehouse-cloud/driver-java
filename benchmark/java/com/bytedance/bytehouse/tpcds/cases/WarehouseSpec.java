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
package com.bytedance.bytehouse.tpcds.cases;

import com.bytedance.bytehouse.tpcds.TpcdsSpec;
import com.bytedance.bytehouse.tpcds.TpcdsBenchmarkSetupUtil;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class WarehouseSpec implements TpcdsSpec {

    public static final TpcdsSpec INSTANCE = new WarehouseSpec();

    private WarehouseSpec() {

    }

    @Override
    public String databaseName() {
        return "tpcds_insert_db";
    }

    @Override
    public String tableName() {
        return "warehouse";
    }

    @Override
    public String createTableSql() {
        return String.format("CREATE TABLE %s.%s (\n" +
                             "    w_warehouse_sk           Nullable(Int64),\n" +
                             "    w_warehouse_id           Nullable(String),\n" +
                             "    w_warehouse_name         Nullable(String),\n" +
                             "    w_warehouse_sq_ft        Nullable(Int64),\n" +
                             "    w_street_number          Nullable(String),\n" +
                             "    w_street_name            Nullable(String),\n" +
                             "    w_street_type            Nullable(String),\n" +
                             "    w_suite_number           Nullable(String),\n" +
                             "    w_city                   Nullable(String),\n" +
                             "    w_county                 Nullable(String),\n" +
                             "    w_state                  Nullable(String),\n" +
                             "    w_zip                    Nullable(String),\n" +
                             "    w_country                Nullable(String),\n" +
                             "    w_gmt_offset             Nullable(Float32)\n" +
                             ") ", databaseName(), tableName());
    }

    @Override
    public String insertSql() {
        return String.format("INSERT INTO %s.%s (w_warehouse_sk, w_warehouse_id, w_warehouse_name, w_warehouse_sq_ft, " +
                        "w_street_number, w_street_name, w_street_type, w_suite_number, w_city, w_country, w_state, w_zip, w_country, w_gmt_offset) " +
                        "VALUES (%s)",
                databaseName(), tableName(), TpcdsBenchmarkSetupUtil.questionMarks(14));
    }

    @Override
    public String selectSql(int limit) {
        return String.format("SELECT * FROM %s.%s LIMIT %d", databaseName(), tableName(), limit);
    }

    @Override
    public String dataFile() {
        return "data/tpcds/warehouse.csv";
    }

    @Override
    public char dataFileColSeparator() {
        return '|';
    }

    @Override
    public void addRow(final PreparedStatement preparedStatement, final String[] row) throws SQLException {
        preparedStatement.setLong(1, Long.parseLong(row[0]));
        preparedStatement.setString(2, row[1]);
        preparedStatement.setString(3, row[2]);
        preparedStatement.setLong(4, Long.parseLong(row[3]));
        preparedStatement.setString(5, row[4]);
        preparedStatement.setString(6, row[5]);
        preparedStatement.setString(7, row[6]);
        preparedStatement.setString(8, row[7]);
        preparedStatement.setString(9, row[8]);
        preparedStatement.setString(10, row[9]);
        preparedStatement.setString(11, row[10]);
        preparedStatement.setString(12, row[11]);
        preparedStatement.setString(13, row[12]);
        preparedStatement.setFloat(14, Float.parseFloat(row[13]));
        preparedStatement.addBatch();
    }
}
