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
package com.bytedance.bytehouse.jdbc.wrapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Some helper methods regarding sql.
 */
public interface SQLHelper {

    /**
     * simulate the sql 'LIKE' matching.
     * null pattern = .* according to sql standard.
     */
    default boolean sqlLike(final String str, final String pattern) {
        if (str == null) return false;
        if (pattern == null) return true;
        return str.toLowerCase().matches(
                pattern.toLowerCase()
                        .replaceAll("\\?", ".")
                        .replaceAll("%", ".*")
        );
    }

    default List<String> rsStringToList(
            final String selectedCol,
            final ResultSet rs
    ) throws SQLException {
        final List<String> output = new ArrayList<>();
        while (rs.next()) {
            output.add(rs.getString(selectedCol));
        }
        return output;
    }
}
