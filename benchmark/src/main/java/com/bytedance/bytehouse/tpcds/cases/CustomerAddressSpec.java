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

public final class CustomerAddressSpec implements TpcdsSpec {

    public static final TpcdsSpec INSTANCE = new CustomerAddressSpec();

    private CustomerAddressSpec() {

    }

    @Override
    public String databaseName() {
        return "tpcds_insert_db";
    }

    @Override
    public String tableName() {
        return "customer_address";
    }

    @Override
    public String createTableSql() {
        return String.format("CREATE TABLE %s.%s (\n" +
                             "    ca_address_sk            Nullable(Int64),\n" +
                             "    ca_address_id            Nullable(String),\n" +
                             "    ca_street_number         Nullable(String),\n" +
                             "    ca_street_name           Nullable(String),\n" +
                             "    ca_street_type           Nullable(String),\n" +
                             "    ca_suite_number          Nullable(String),\n" +
                             "    ca_city                  Nullable(String),\n" +
                             "    ca_county                Nullable(String),\n" +
                             "    ca_state                 Nullable(String),\n" +
                             "    ca_zip                   Nullable(String),\n" +
                             "    ca_country               Nullable(String),\n" +
                             "    ca_gmt_offset            Nullable(Float32),\n" +
                             "    ca_location_type         Nullable(String)\n" +
                             ") ", databaseName(), tableName());
    }

    @Override
    public String insertSql() {
        return String.format("INSERT INTO %s.%s (ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, " +
                    "ca_suite_number, ca_city, ca_country, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type) " +
                    "VALUES (%s)",
                databaseName(), tableName(), TpcdsBenchmarkSetupUtil.questionMarks(13));
    }

    @Override
    public String selectSql(int limit) {
        return String.format("SELECT * FROM %s.%s LIMIT %d", databaseName(), tableName(), limit);
    }

    @Override
    public String dataFile() {
        return "data/tpcds/customer_address.csv";
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
        preparedStatement.setString(4, row[3]);
        preparedStatement.setString(5, row[4]);
        preparedStatement.setString(6, row[5]);
        preparedStatement.setString(7, row[6]);
        preparedStatement.setString(8, row[7]);
        preparedStatement.setString(9, row[8]);
        preparedStatement.setString(10, row[9]);
        preparedStatement.setString(11, row[10]);
        preparedStatement.setFloat(12, Float.parseFloat(row[11]));
        preparedStatement.setString(13, row[12]);
        preparedStatement.addBatch();
    }
}
