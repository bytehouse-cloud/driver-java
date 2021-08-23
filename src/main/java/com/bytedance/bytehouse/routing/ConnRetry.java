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

import com.bytedance.bytehouse.jdbc.CnchRoutingDataSource;
import com.bytedance.bytehouse.log.Logger;
import com.bytedance.bytehouse.log.LoggerFactory;
import com.google.common.annotations.VisibleForTesting;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Wrapper class to execute arbitrary logic with {@link CnchRoutingDataSource}. <br>
 * This wrapper intercepts and {@link SQLException} and decide if the exception
 * should be thrown out or retry should be performed.
 */
public class ConnRetry {

    private static final Logger LOG = LoggerFactory.getLogger(ConnRetry.class);

    private static final Pattern CODE_PATTERN = Pattern
            .compile("[Cc]ode:\\s([0-9]+)[,.]{1}");

    /**
     * Error Codes are filtered from <a href="https://code.byted.org/dp/ClickHouse/blob/cnch_dev/dbms/src/Common/ErrorCodes.cpp">ErrorCodes.cpp</a>.
     */
    private static final Set<Integer> UNRECOVERABLE_CODES = new HashSet<>(
            Arrays.asList(1, 2, 4, 6, 7, 8, 9, 11, 12,
                    13, 15, 16, 17, 19, 20, 32, 34, 35, 36, 37, 39, 40, 41, 42, 43, 44, 45, 46,
                    47, 48, 49, 50, 51, 52, 53, 57, 58, 59, 60, 61, 62, 63, 66, 67, 68, 69, 70, 72,
                    73, 74, 75, 78));

    @VisibleForTesting
    long accumulatedWaitTimeMs;

    private NextWaitTime defaultPolicy = (max, curr, accumulated, attemptCount) -> Math.min(
            curr * 2,
            max - accumulated
    );

    private long currentSleepDurationMs = 200;

    private long totalWaitTimeMaxMs = 10 * 1000;

    private CnchRoutingDataSource dataSource;

    private int attemptCountCurr;

    private String tableUuid;

    @VisibleForTesting
    ConnRetry() {
        // should not be called outside this class.
    }

    /**
     * The starting point. Accepts the {@link Connection} factory.
     *
     * @param dataSource connection factory to use.
     */
    public static ConnRetry withFactory(final CnchRoutingDataSource dataSource) {
        Objects.requireNonNull(dataSource, "connection factory cannot be null");
        final ConnRetry retry = new ConnRetry();
        retry.dataSource = dataSource;
        return retry;
    }

    /**
     * set the maximum backoff waiting time before another retry.
     */
    public ConnRetry withBaseSleepDuration(final long durationMs) {
        final int minValid = 1;
        if (durationMs < minValid) {
            throw new IllegalArgumentException("base sleep duration cannot be less than "
                    + minValid);
        }
        this.currentSleepDurationMs = durationMs;
        return this;
    }

    /**
     * set the maximum backoff waiting time before another retry.
     */
    public ConnRetry withTotalMaxWaitTimeMs(final long max) {
        final int minValid = 1;
        if (max < minValid) {
            throw new IllegalArgumentException("backoff time max cannot be less than "
                    + minValid);
        }
        this.totalWaitTimeMaxMs = max;
        return this;
    }

    /**
     * The algorithm to calculate the next waiting time.
     */
    public ConnRetry withIncrementPolicy(final NextWaitTime policy) {
        Objects.requireNonNull(policy, "Policy cannot be null");
        this.defaultPolicy = policy;
        return this;
    }

    /**
     * Set the tableUUID for write connection().
     */
    public ConnRetry withTableUuid(final String uuid) {
        this.tableUuid = uuid;
        return this;
    }

    /**
     * This method wraps around a lambda to let users execute their arbitrary logic
     * with {@link Connection}. The {@link SQLException} is intercepted before passing
     * back to the caller.
     * <br><br>
     * If the {@link SQLException} is a result of connection time
     * out related error, the exception is swallowed and retry logic is applied with a backoff.
     * <br><br>
     * If the {@link SQLException} is a result of cnch topology issue, the exception
     * is swallowed and retry logic is applied with the new topology.
     * <br><br>
     * <b>Important</b>: do not return {@link AutoCloseable} objects from the connection since
     * the connection will be closed when the function returns. <br>
     * e.g. Do not return {@link java.sql.ResultSet}
     *
     * <br><br><br>
     * if you do not wish to return anything, return Void.class. example: <br>
     *
     * <pre>
     * ConnRetry.withFactory(connManager).run(conn -> {
     *     PreparedStatement pstmt = conn.prepareStatement(sql);
     *     pstmt.executeQuery();
     * });
     * </pre>
     * or if you need to return something
     * <pre>
     * ConnRetry.&lt;Integer&gt;withFactory(connManager).run(conn -> {
     *     PreparedStatement pstmt = conn.prepareStatement(sql);
     *     pstmt.executeQuery();
     *     return 1;
     * });
     * </pre>
     */
    public <RESULT> RESULT run(final Expression<RESULT> expression) throws SQLException {
        SQLException lastKnownException = new SQLException("failure after retries");
        while (!Thread.currentThread().isInterrupted()) {
            if (accumulatedWaitTimeMs >= totalWaitTimeMaxMs) {
                LOG.info("Giving up reattempting after {} milliseconds", accumulatedWaitTimeMs);
                break;
            }

            // second time onwards, we wait before executing
            if (conditionalPauseBeforeExecution()) {
                break;
            }

            try {
                try (Connection connection = dataSource.getConnection(tableUuid)) {
                    return expression.apply(connection);
                }
            } catch (SQLException ex) {
                if (!isRetryableException(ex)) {
                    throw ex;
                }

                LOG.warn("attempt {} failed with exception ", attemptCountCurr, ex);
                lastKnownException = ex;
                dataSource.unloadTopology();
                attemptCountCurr++;
            }
        }
        throw lastKnownException;
    }

    /**
     * For the case that nothing needs to be returned.
     */
    public void run(final Statement statement) throws SQLException {
        this.run(conn -> {
            statement.consume(conn);
            return null;
        });
    }

    private boolean conditionalPauseBeforeExecution() {
        if (attemptCountCurr > 0) {
            LOG.info("Encountered exception. Retry after {} millisecond", currentSleepDurationMs);

            if (!sleep(currentSleepDurationMs)) {
                return true;
            }

            currentSleepDurationMs = defaultPolicy.calculate(
                    totalWaitTimeMaxMs,
                    currentSleepDurationMs,
                    accumulatedWaitTimeMs,
                    attemptCountCurr
            );
        }
        return false;
    }

    /**
     * 11th May 2021:
     * Other than listed error code, the rest are considered recoverable.
     */
    boolean isRetryableException(final SQLException exception) {
        final String message = exception.getMessage();
        final Matcher matcher = CODE_PATTERN.matcher(message);
        if (matcher.find()) {
            final String codeValue = matcher.group(1);
            if (codeValue == null) {
                LOG.error("Unable to get error code from the exception: {}", message);
                return true;
            } else {
                try {
                    final Integer integer = Integer.valueOf(codeValue);
                    return !UNRECOVERABLE_CODES.contains(integer);
                } catch (NumberFormatException e) {
                    LOG.error("Unable to get error code from the exception: {}", message);
                    return true;
                }
            }
        } else {
            LOG.error("Unable to get error code from the exception: {}", message);
            return true;
        }
    }

    private boolean sleep(final long durationMs) {
        try {
            Thread.sleep(durationMs);
            accumulatedWaitTimeMs += durationMs;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
        return true;
    }

    /**
     * Execute arbitrary with {@link Connection}.
     *
     * @param <OUT> output type.
     */
    @FunctionalInterface
    public interface Expression<OUT> {

        /**
         * consume connection.
         */
        OUT apply(Connection conn) throws SQLException;
    }

    /**
     * Execute arbitrary with {@link Connection}.
     */
    @FunctionalInterface
    public interface Statement {

        /**
         * consume connection.
         */
        void consume(Connection conn) throws SQLException;
    }

    /**
     * The algorithm to calculate the next waiting time.
     */
    @FunctionalInterface
    public interface NextWaitTime {

        /**
         * calculate.
         *
         * @param maxMs         maximum waiting time in millisecond
         * @param currMs        current waiting time in millisecond
         * @param accumulatedMs accumulated waiting time so far in millisecond
         * @param attemptCount  attempt count so far.
         * @return next waiting time
         */
        long calculate(long maxMs, long currMs, long accumulatedMs, int attemptCount);
    }
}
