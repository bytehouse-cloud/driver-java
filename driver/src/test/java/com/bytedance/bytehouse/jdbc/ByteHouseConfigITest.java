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
package com.bytedance.bytehouse.jdbc;

import com.bytedance.bytehouse.jdbc.statement.ByteHouseStatement;
import com.bytedance.bytehouse.settings.ByteHouseConfig;
import com.bytedance.bytehouse.settings.SettingKey;
import org.junit.jupiter.api.Test;
import java.time.Duration;

public class ByteHouseConfigITest {
    @Test
    public void buildConnectionWithConfig() throws Exception {
        ByteHouseConfig cfg = ByteHouseConfig.Builder.builder()
                .charset("UTF-8")
                .secure(true)
                .region("CN-NORTH-1-STAGING")
                .withSetting(SettingKey.send_timeout, Duration.ofMinutes(5))
                .build()
                .withCredentials("zx", "P`55word")
                .withAccount("AWSLJEWV");

        ByteHouseConnection connection = new ByteHouseDriver().connect("jdbc:bytehouse:///?region=CN-NORTH-1-STAGING", cfg);
        ByteHouseStatement statement = (ByteHouseStatement) connection.createStatement();
        statement.execute("CREATE DATABASE IF NOT EXISTS testraf");
    }
}
