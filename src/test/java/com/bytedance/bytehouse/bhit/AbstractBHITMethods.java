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
package com.bytedance.bytehouse.bhit;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;

public abstract class AbstractBHITMethods extends AbstractBHITEnvironment {

    protected void withNewConnection(WithConnection withConnection) throws Exception {
        try (Connection connection = getEnvConnection()) {
            withConnection.apply(connection);
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
}
