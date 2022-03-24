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
package com.bytedance.bytehouse.stream;

import com.bytedance.bytehouse.data.Block;
import com.bytedance.bytehouse.misc.SQLLexer;
import com.bytedance.bytehouse.misc.ValidateUtils;
import java.sql.SQLException;
import java.util.BitSet;

public class ValuesWithParametersNativeInputFormat implements NativeInputFormat {

    private final SQLLexer lexer;

    public ValuesWithParametersNativeInputFormat(int pos, String sql) {
        this.lexer = new SQLLexer(pos, sql);
    }

    @Override
    public void fill(final Block block) throws SQLException {
        final BitSet constIdxFlags = new BitSet(block.columnCnt());
        char nextChar = lexer.character();
        ValidateUtils.isTrue(nextChar == '(');
        for (int columnIdx = 0; columnIdx < block.columnCnt(); columnIdx++) {
            if (columnIdx > 0) {
                ValidateUtils.isTrue(lexer.character() == ',');
            }

            if (lexer.isCharacter('?')) {
                lexer.character();
            } else {
                constIdxFlags.set(columnIdx);
                block.setObject(columnIdx, block.getColumn(columnIdx).type().deserializeText(lexer));
            }
        }

        int placeholderSeen = 0;
        for (int columnIdx = 0; columnIdx < block.columnCnt(); columnIdx++) {
            if (!constIdxFlags.get(columnIdx)) {
                block.updatePlaceholderIndex(placeholderSeen, columnIdx);
                placeholderSeen++;
            }
        }
        ValidateUtils.isTrue(lexer.character() == ')');
    }
}
