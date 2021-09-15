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

import com.bytedance.bytehouse.tpcds.AbstractSelectTBenchmark;
import com.bytedance.bytehouse.tpcds.TpcdsSpec;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import java.sql.SQLException;

public class WarehouseSelectTBenchmark extends AbstractSelectTBenchmark {

    private final TpcdsSpec tpcdsSpec = WarehouseSpec.INSTANCE;

    @Param({"50"})
    protected int batchCount = 1;

    @Param({"200000"})
    protected int batchSize = 1;

    @Setup
    public void setup() throws Exception {
        initForSelect(tpcdsSpec, batchSize);
    }

    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @Fork(value = 1, warmups = 0)
    @Warmup(iterations = 2)
    @Measurement(iterations = 5)
    public void batchSelect(Blackhole blackhole) throws Exception {
        selectRows(tpcdsSpec, batchCount, batchSize, blackhole);
    }

    @TearDown
    public void teardown() throws SQLException {
        teardown(tpcdsSpec.databaseName());
    }
}
