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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.junit.jupiter.api.Test;

class JumpConsistentHashTest {

    @Test
    void consistentHashForString_outputSameAsCpp() throws IOException {

        // read answer
        List<Integer> ans = new ArrayList<>();
        try (final BufferedReader br = new BufferedReader(new InputStreamReader(
                Objects.requireNonNull(
                        Test.class.getResourceAsStream("/consistentHashingCpp/test_output.txt")
                )
        ))) {
            String line;
            while ((line = br.readLine()) != null) {
                ans.add(Integer.valueOf(line));
            }
        }

        // check answer
        int itt = 0;
        try (final BufferedReader br = new BufferedReader(new InputStreamReader(
                Objects.requireNonNull(
                        Test.class.getResourceAsStream("/consistentHashingCpp/test_input.txt")
                )
        ))) {
            String line;
            while ((line = br.readLine()) != null) {
                final int i = JumpConsistentHash.consistentHashForString(line, Integer.MAX_VALUE);
                assertThat(i)
                        .as("Hashing of %s is incorrect", line)
                        .isEqualTo(ans.get(itt++));
            }
        }
    }
}
