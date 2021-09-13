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

package com.bytedance.bytehouse.clickhouse;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;
import com.bytedance.bytehouse.AbstractIBenchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
public class AbstractClickHouseBenchmark extends AbstractIBenchmark {

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(".*")
                .warmupIterations(0)
                .measurementIterations(1)
                .forks(2)
                .build();

        new Runner(opt).run();
    }

    protected void withConnection(WithConnection withConnection, ConnectionType connectionType) throws Exception {
        try (Connection connection = getConnection()) {
            withConnection.apply(connection);
        }
    }

    protected void withStatement(Connection connection, WithStatement withStatement) throws Exception {
        try (Statement stmt = getStatement()) {
            withStatement.apply(stmt);
        }
    }

    protected void withPreparedStatement(Connection connection,
                                         String sql,
                                         WithPreparedStatement withPreparedStatement) throws Exception {
        try (PreparedStatement pstmt = getPreparedStatement(sql)) {
            withPreparedStatement.apply(pstmt);
        }
    }

    @FunctionalInterface
    public interface WithConnection {
        void apply(Connection connection) throws Exception;
    }

    @FunctionalInterface
    public interface WithStatement {
        void apply(Statement stmt) throws Exception;
    }

    @FunctionalInterface
    public interface WithPreparedStatement {
        void apply(PreparedStatement pstmt) throws Exception;
    }

    public enum ConnectionType {
        NATIVE
    }
}
