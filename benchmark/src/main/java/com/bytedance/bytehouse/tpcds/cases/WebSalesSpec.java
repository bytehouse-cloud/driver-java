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

public class WebSalesSpec implements TpcdsSpec {

    public static final TpcdsSpec INSTANCE = new WebSalesSpec();

    private WebSalesSpec() {

    }

    @Override
    public String databaseName() {
        return "tpcds_insert_db";
    }

    @Override
    public String tableName() {
        return "web_sales";
    }

    @Override
    public String createTableSql() {
        return String.format("CREATE TABLE %s.%s\n" +
                             "(\n" +
                             "    `ws_bill_addr_sk` Nullable(Int64),\n" +
                             "    `ws_bill_cdemo_sk` Nullable(Int64),\n" +
                             "    `ws_bill_customer_sk` Nullable(Int64),\n" +
                             "    `ws_bill_hdemo_sk` Nullable(Int64),\n" +
                             "    `ws_coupon_amt` Nullable(Float32),\n" +
                             "    `ws_ext_discount_amt` Nullable(Float32),\n" +
                             "    `ws_ext_list_price` Nullable(Float32),\n" +
                             "    `ws_ext_sales_price` Nullable(Float32),\n" +
                             "    `ws_ext_ship_cost` Nullable(Float32),\n" +
                             "    `ws_ext_tax` Nullable(Float32),\n" +
                             "    `ws_ext_wholesale_cost` Nullable(Float32),\n" +
                             "    `ws_item_sk` Nullable(Int64),\n" +
                             "    `ws_list_price` Nullable(Float32),\n" +
                             "    `ws_net_paid` Nullable(Float32),\n" +
                             "    `ws_net_paid_inc_ship` Nullable(Float32),\n" +
                             "    `ws_net_paid_inc_ship_tax` Nullable(Float32),\n" +
                             "    `ws_net_paid_inc_tax` Nullable(Float32),\n" +
                             "    `ws_net_profit` Nullable(Float32),\n" +
                             "    `ws_order_number` Nullable(Int64),\n" +
                             "    `ws_promo_sk` Nullable(Int64),\n" +
                             "    `ws_quantity` Nullable(Int64),\n" +
                             "    `ws_sales_price` Nullable(Float32),\n" +
                             "    `ws_ship_addr_sk` Nullable(Int64),\n" +
                             "    `ws_ship_cdemo_sk` Nullable(Int64),\n" +
                             "    `ws_ship_customer_sk` Nullable(Int64),\n" +
                             "    `ws_ship_date_sk` Nullable(Int64),\n" +
                             "    `ws_ship_hdemo_sk` Nullable(Int64),\n" +
                             "    `ws_ship_mode_sk` Nullable(Int64),\n" +
                             "    `ws_sold_date_sk` Nullable(Int64),\n" +
                             "    `ws_sold_time_sk` Nullable(Int64),\n" +
                             "    `ws_warehouse_sk` Nullable(Int64),\n" +
                             "    `ws_web_page_sk` Nullable(Int64),\n" +
                             "    `ws_web_site_sk` Nullable(Int64),\n" +
                             "    `ws_wholesale_cost` Nullable(Float32)\n" +
                             ") ", databaseName(), tableName());
    }

    @Override
    public String insertSql() {
        return String.format("INSERT INTO %s.%s (ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, " +
                        "ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, " +
                        "ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, " +
                        "ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, " +
                        "ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, " +
                        "ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, " +
                        "ws_net_paid_inc_ship_tax, ws_net_profit) " +
                        "VALUES (%s)",
                databaseName(), tableName(), TpcdsBenchmarkSetupUtil.questionMarks(34));
    }

    @Override
    public String selectSql(int limit) {
        return String.format("SELECT * FROM %s.%s LIMIT %d", databaseName(), tableName(), limit);
    }

    @Override
    public String dataFile() {
        return "data/tpcds/web_sales.csv";
    }

    @Override
    public char dataFileColSeparator() {
        return '|';
    }

    @Override
    public void addRow(final PreparedStatement preparedStatement, final String[] row) throws SQLException {
        for (int j = 0; j < 19; j++) {
            preparedStatement.setLong(j + 1, Long.parseLong(row[j]));
        }
        for (int j = 19; j < 34; j++) {
            preparedStatement.setFloat(j + 1, Float.parseFloat(row[j]));
        }
        preparedStatement.addBatch();
    }
}
