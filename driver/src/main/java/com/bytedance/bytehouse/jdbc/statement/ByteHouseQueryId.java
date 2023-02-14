/*
 * This file may have been modified by ByteDance Ltd. and/or its affiliates.
 *
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
package com.bytedance.bytehouse.jdbc.statement;

import com.bytedance.bytehouse.log.Logger;
import com.bytedance.bytehouse.log.LoggerFactoryUtils;
import java.util.UUID;

public abstract class ByteHouseQueryId {
    private static final Logger LOG = LoggerFactoryUtils.getLogger(ByteHouseQueryId.class);
    private String lastQueryId = "";
    private boolean queryIdUsed = true;


    public String getQueryId() {
        return lastQueryId;
    }

    public void setQueryId(String queryId) {
        lastQueryId = queryId;
        queryIdUsed = false;
    }

    protected String consumeQueryId() {
        if (!this.queryIdUsed) {
            queryIdUsed = true;
        } else {
            lastQueryId = UUID.randomUUID().toString();
        }
        // Example: DEBUG ByteHouseQueryId - 3dc5fe33-7c6a-4ab1-9125-6884dd5633e9
        LOG.debug("{}", lastQueryId);
        return lastQueryId;
    }
}
