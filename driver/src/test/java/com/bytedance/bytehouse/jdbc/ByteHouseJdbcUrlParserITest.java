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

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.bytedance.bytehouse.settings.SettingKey;
import java.io.Serializable;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Test;

public class ByteHouseJdbcUrlParserITest {

    @Test
    public void parseJdbcUrl_succeed() {
        Map<SettingKey, Serializable> settings = ByteHouseJdbcUrlParser.parseJdbcUrl(
                "jdbc:bytehouse://myhost:8000/database"
        );

        assertEquals(settings.size(), 3);
        assertEquals(settings.get(SettingKey.host), "myhost");
        assertEquals(settings.get(SettingKey.port), 8000);
        assertEquals(settings.get(SettingKey.database), "database");
    }

    @Test
    public void parseJdbcUrl_cnchInScheme() {
        Map<SettingKey, Serializable> settings = ByteHouseJdbcUrlParser.parseJdbcUrl(
                "jdbc:cnch://myhost:8000/database"
        );

        assertEquals(settings.size(), 3);
        assertEquals(settings.get(SettingKey.host), "myhost");
        assertEquals(settings.get(SettingKey.port), 8000);
        assertEquals(settings.get(SettingKey.database), "database");
    }

    @Test
    public void parseJdbcUrl_withValidQueryParameters_succeed() {
        Map<SettingKey, Serializable> settings = ByteHouseJdbcUrlParser.parseJdbcUrl(
                "jdbc:bytehouse://myhost:8000/database?user=person&password=P@ssword"
        );

        assertEquals(settings.size(), 5);
        assertEquals(settings.get(SettingKey.host), "myhost");
        assertEquals(settings.get(SettingKey.port), 8000);
        assertEquals(settings.get(SettingKey.database), "database");
        assertEquals(settings.get(SettingKey.user), "person");
        assertEquals(settings.get(SettingKey.password), "P@ssword");
    }

    @Test
    public void parseJdbcUrl_withInvalidQueryParameters_shouldIgnore() {
        Map<SettingKey, Serializable> settings = ByteHouseJdbcUrlParser.parseJdbcUrl(
                "jdbc:bytehouse://myhost:8000/database?random=string"
        );

        assertEquals(settings.size(), 3 );
        assertEquals(settings.get(SettingKey.host), "myhost");
        assertEquals(settings.get(SettingKey.port), 8000);
        assertEquals(settings.get(SettingKey.database), "database");
    }

    @Test
    public void parseProperties_withValidSettingKey_succeed() {
        Properties properties = new Properties();
        properties.setProperty(SettingKey.skipVerification.name(), "true");
        properties.setProperty(SettingKey.user.name(), "username");

        Map<SettingKey, Serializable> settings = ByteHouseJdbcUrlParser.parseProperties(properties);

        assertEquals(settings.size(), 2);
        assertEquals(settings.get(SettingKey.skipVerification), true);
        assertEquals(settings.get(SettingKey.user), "username");
    }

    @Test
    public void parseProperties_withInvalidSettingKey_shouldIgnore() {
        Properties properties = new Properties();
        properties.setProperty("random", "string");
        Map<SettingKey, Serializable> settings = ByteHouseJdbcUrlParser.parseProperties(properties);
        assertEquals(settings.size(), 0);
    }
}
