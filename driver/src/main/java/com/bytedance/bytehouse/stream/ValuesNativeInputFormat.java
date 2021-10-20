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

/**
 * Fills the {@link Block} with the data from behind the "values" syntax.
 * <br>
 * For example:<br>
 * <pre>
 * insert into table xxx (col1, col2) values (1,2), (3,4)
 * </pre>
 * will become
 * <pre>
 *  block
 *  ---------
 *  | 1 | 2 |
 *  ---------
 *  | 3 | 4 |
 *  ---------
 * </pre>
 */
public class ValuesNativeInputFormat implements NativeInputFormat {

    private final SQLLexer lexer;

    /**
     * create a input form.
     */
    public ValuesNativeInputFormat(final int pos, final String sql) {
        this.lexer = new SQLLexer(pos, sql);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void fill(Block block) throws SQLException {
        final BitSet constIdxFlags = new BitSet(block.columnCnt());
        while (true) {
            char nextChar = lexer.character();
            if (lexer.eof() || nextChar == ';') {
                break;
            }

            if (nextChar == ',') {
                nextChar = lexer.character();
            }
            ValidateUtils.isTrue(nextChar == '(');
            for (int columnIdx = 0; columnIdx < block.columnCnt(); columnIdx++) {
                if (columnIdx > 0) {
                    ValidateUtils.isTrue(lexer.character() == ',');
                }
                constIdxFlags.set(columnIdx);
                block.setObject(columnIdx, block.getColumn(columnIdx).type().deserializeText(lexer));
            }
            ValidateUtils.isTrue(lexer.character() == ')');
            block.appendRow();
        }

        for (int columnIdx = 0; columnIdx < block.columnCnt(); columnIdx++) {
            if (constIdxFlags.get(columnIdx)) {
                block.incPlaceholderIndexes(columnIdx);
            }
        }
    }
}
