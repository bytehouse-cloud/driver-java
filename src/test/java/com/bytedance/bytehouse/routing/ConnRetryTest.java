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
package com.bytedance.bytehouse.routing;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.bytedance.bytehouse.jdbc.ByteHouseConnection;
import com.bytedance.bytehouse.jdbc.CnchRoutingDataSource;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ConnRetryTest {

    final SQLException topologyException = new SQLException("Code: 5034. DB::Exception: Received "
            + "from 10.231.11.22:10000. DB::Exception: Cannot commit parts because of choosing "
            + "wrong server according to current topology, chosen server: 172.18.77.235:8124, "
            + "timestamp: 424839752596066476.");

    final SQLException timeoutException = new SQLException("ru.yandex.clickhouse.except."
            + "ClickHouseException: ClickHouse exception, code: 159, host:"
            + " cnch-server-default.cnch.svc.cluster.local, port: 8123; Read timed out");

    final SQLException miscException = new SQLException("3049480682096022946.yilun, "
            + "Table: job_113_kafka_consumer_1620357787963553630_0, Consumer: 0, Metric: 0, "
            + "Bytes: 0, HasError: 1, Exception: Code: 7005, e.displayText() = DB::Exception: "
            + "DB::Exception: Unable to perform timestamp operation: GET_TIMESTAMP. "
            + "Please retry the query after a few seconds..: while write suffix to "
            + "view 3049480682096022946.yilun");

    final SQLException tableNotFoundException = new SQLException("Code: 60. DB::Exception: "
            + "Received from 10.231.11.22:10000. DB::Exception: Table dataexpress.np doesn't "
            + "exist or is detached..");

    final SQLException noErrorCodeException = new SQLException("DB::Exception: Received "
            + "from 10.231.11.22:10000. DB::Exception: Table dataexpress.np doesn't exist "
            + "or is detached..");

    final SQLException malformedErrorCodeException = new SQLException("code 1234566789999999,"
            + "DB::Exception: Received "
            + "from 10.231.11.22:10000. DB::Exception: Table dataexpress.np doesn't exist "
            + "or is detached..");

    CnchRoutingDataSource factory = mock(CnchRoutingDataSource.class);

    ByteHouseConnection mockConn = mock(ByteHouseConnection.class);

    @BeforeEach
    void setUp() throws SQLException {
        when(factory.getConnection()).thenReturn(mockConn);
        when(factory.getConnection(anyString())).thenReturn(mockConn);
    }

    @Test
    void isNonRetryableException_givenDifferentCases_returnTrueOnlyForWhiteListed() {
        assertThat(
                new ConnRetry().isRetryableException(topologyException)
        ).isTrue();
        assertThat(
                new ConnRetry().isRetryableException(timeoutException)
        ).isTrue();
        assertThat(
                new ConnRetry().isRetryableException(miscException)
        ).isTrue();
        assertThat(
                new ConnRetry().isRetryableException(tableNotFoundException)
        ).isFalse();
        assertThat(
                new ConnRetry().isRetryableException(noErrorCodeException)
        ).isTrue();
        assertThat(
                new ConnRetry().isRetryableException(malformedErrorCodeException)
        ).isTrue();
    }

    @Test
    void run_withMaxTime_shouldNotExceedByMuch() throws SQLException {
        long total = 100;
        AtomicInteger counter = new AtomicInteger(0);
        final ConnRetry retry = ConnRetry.withFactory(factory)
                .withTotalMaxWaitTimeMs(total)
                .withBaseSleepDuration(1);
        final SQLException ex = assertThrows(SQLException.class, () -> {
            retry
                    .run(conn -> {
                        counter.incrementAndGet();
                        if (true) {
                            throw topologyException;
                        }
                    });
        });

        assertThat(retry.accumulatedWaitTimeMs).as("Time taken should be around the "
                + "time limit").isBetween(total, (long) (total * 1.05));
        assertThat(counter.get()).as("Retry should happen")
                .isGreaterThan(1);
        assertThat(ex).as("Last exception should be thrown out")
                .isEqualTo(topologyException);
    }

    @Test
    void run_nonRetryableException_shouldThrowOutImmediately() {
        long total = 100;
        AtomicInteger counter = new AtomicInteger(0);
        final ConnRetry retry = ConnRetry.withFactory(factory)
                .withTotalMaxWaitTimeMs(total)
                .withBaseSleepDuration(1);
        final SQLException ex = assertThrows(SQLException.class, () -> {
            retry
                    .run(conn -> {
                        counter.incrementAndGet();
                        if (true) {
                            throw tableNotFoundException;
                        }
                    });
        });

        assertThat(retry.accumulatedWaitTimeMs).as("Time taken should be zero").isZero();
        assertThat(counter.get()).as("One single attempt")
                .isEqualTo(1);
        assertThat(ex).as("Last exception should be thrown out")
                .isEqualTo(tableNotFoundException);
    }

    @Test
    void run_nonSqlException_shouldThrowOutImmediately() {
        long total = 100;
        AtomicInteger counter = new AtomicInteger(0);
        final RuntimeException runtimeException = new RuntimeException("Hey there");
        final ConnRetry retry = ConnRetry.withFactory(factory)
                .withTotalMaxWaitTimeMs(total)
                .withBaseSleepDuration(1);
        final Exception ex = assertThrows(Exception.class, () -> {
            retry
                    .run(conn -> {
                        counter.incrementAndGet();
                        if (true) {
                            throw runtimeException;
                        }
                    });
        });

        assertThat(retry.accumulatedWaitTimeMs).as("Time taken should be zero").isZero();
        assertThat(counter.get()).as("One single attempt")
                .isEqualTo(1);
        assertThat(ex).as("Last exception should be thrown out")
                .isEqualTo(runtimeException);
    }
}
