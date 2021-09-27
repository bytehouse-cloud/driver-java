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
package com.bytedance.bytehouse.jdbc.drivers;

import com.bytedance.bytehouse.jdbc.AbstractITest;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Ignore;
import ru.yandex.clickhouse.domain.ClickHouseDataType;
import ru.yandex.clickhouse.util.ClickHouseBitmap;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.*;

import static org.testng.Assert.*;

public class ByteHouseBitmapTest extends AbstractITest {
    private Connection conn;

    @BeforeTest
    public void setUp() throws Exception {
        conn = getConnection();
    }

    @AfterTest
    public void tearDown() throws Exception {
        if (conn != null) {
            conn.close();
        }
    }

    private void checkBitmaps(ResultSet rs, ClickHouseBitmap empty, ClickHouseBitmap small, ClickHouseBitmap large)
            throws SQLException {
        // Charset charset = Charset.forName("ISO-8859-15");
        Charset charset = StandardCharsets.UTF_8;

        assertTrue(rs.next());
        assertEquals(rs.getObject(1), 0L);
        assertEquals(rs.getString(2), new String(empty.toBytes(), charset));
        assertEquals(rs.getObject(2), rs.getObject(2, ClickHouseBitmap.class));
        assertEquals(rs.getObject(2, ClickHouseBitmap.class), empty);

        assertTrue(rs.next());
        assertEquals(rs.getObject(1), 1L);
        assertEquals(rs.getString(2), new String(small.toBytes(), charset));
        assertEquals(rs.getObject(2), rs.getObject(2, ClickHouseBitmap.class));
        assertEquals(rs.getObject(2, ClickHouseBitmap.class), small);

        assertTrue(rs.next());
        assertEquals(rs.getObject(1), 2L);
        assertEquals(rs.getString(2), new String(large.toBytes(), charset));
        assertEquals(rs.getObject(2), rs.getObject(2, ClickHouseBitmap.class));
        assertEquals(rs.getObject(2, ClickHouseBitmap.class), large);

        assertFalse(rs.next());
    }

    // TODO: Exception -> err=column: unhandled type AggregateFunction(groupBitmap, UInt32)
    @Ignore
    public void testRoaringBitmap() throws Exception {
        if (conn == null) {
            return;
        }

        int[] smallSet = new int[32];
        for (int i = 0, len = smallSet.length; i < len; i++) {
            int value = i + 100;
            smallSet[i] = value;
        }

        int[] largeSet = new int[33];
        for (int i = 0, len = largeSet.length; i < len; i++) {
            int value = i + 60000;
            largeSet[i] = value;
        }

        String testQuery = "select *, base64Encode(toString(rb)) as x from test.test_roaring_bitmap order by i";
        ClickHouseBitmap empty = ClickHouseBitmap.wrap(RoaringBitmap.bitmapOf(), ClickHouseDataType.UInt32);
        ClickHouseBitmap small = ClickHouseBitmap.wrap(smallSet);
        ClickHouseBitmap large = ClickHouseBitmap.wrap(largeSet);
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE DATABASE IF NOT EXISTS test");
            // AggregateFunction(groupBitmap, UInt32) cannot be used inside Nullable type
            // AggregateFunction(groupBitmap, Nullable(UInt32)) can be created but not
            // usable
            s.execute(
                    String.format("CREATE TABLE IF NOT EXISTS test.test_roaring_bitmap(i UInt32, rb AggregateFunction(groupBitmap, UInt32)) %s", getCreateTableSuffix()));
            s.execute("insert into test.test_roaring_bitmap values(0, " + empty.toBitmapBuildExpression() + ")");
            s.execute("insert into test.test_roaring_bitmap values(1, " + small.toBitmapBuildExpression() + ")");
            s.execute("insert into test.test_roaring_bitmap values(2, " + large.toBitmapBuildExpression() + ")");

            try (ResultSet rs = s.executeQuery(testQuery)) {
                checkBitmaps(rs, empty, small, large);
            }

            s.execute("truncate table test.test_roaring_bitmap");
        }

//         FIXME too bad batching is not supported
        try (PreparedStatement s = conn.prepareStatement("insert into test.test_roaring_bitmap values(?,?)")) {
            s.setObject(1, 0L);
            s.setObject(2, empty);
            s.execute();
            s.setObject(1, 1L);
            s.setObject(2, small);
            s.execute();
            s.setObject(1, 2L);
            s.setObject(2, large);
            s.execute();
        }

        try (Statement s = conn.createStatement()) {
            try (ResultSet rs = s.executeQuery(testQuery)) {
                checkBitmaps(rs, empty, small, large);
            }

            s.execute("truncate table test.test_roaring_bitmap");
        }

        try (PreparedStatement s = conn.prepareStatement("insert into test.test_roaring_bitmap values(?,?)")) {
            s.setObject(1, 0L);
            s.setObject(2, ClickHouseBitmap.wrap(MutableRoaringBitmap.bitmapOf(), ClickHouseDataType.UInt32));
            s.execute();
            s.setObject(1, 1L);
            s.setObject(2, ClickHouseBitmap.wrap(MutableRoaringBitmap.bitmapOf(smallSet), ClickHouseDataType.UInt32));
            s.execute();
            s.setObject(1, 2L);
            s.setObject(2, ClickHouseBitmap.wrap(MutableRoaringBitmap.bitmapOf(largeSet), ClickHouseDataType.UInt32));
            s.execute();
        }

        try (Statement s = conn.createStatement()) {
            try (ResultSet rs = s.executeQuery(testQuery)) {
                checkBitmaps(rs, empty, small, large);
            }

            s.execute("truncate table test.test_roaring_bitmap");
        }

        try (PreparedStatement s = conn.prepareStatement("insert into test.test_roaring_bitmap values(?,?)")) {
            s.setObject(1, 0L);
            s.setObject(2, ClickHouseBitmap.wrap(ImmutableRoaringBitmap.bitmapOf(), ClickHouseDataType.UInt32));
            s.execute();
            s.setObject(1, 1L);
            s.setObject(2, ClickHouseBitmap.wrap(ImmutableRoaringBitmap.bitmapOf(smallSet), ClickHouseDataType.UInt32));
            s.execute();
            s.setObject(1, 2L);
            s.setObject(2, ClickHouseBitmap.wrap(ImmutableRoaringBitmap.bitmapOf(largeSet), ClickHouseDataType.UInt32));
            s.execute();
        }

        try (Statement s = conn.createStatement()) {
            try (ResultSet rs = s.executeQuery(testQuery)) {
                checkBitmaps(rs, empty, small, large);
            }

            s.execute("truncate table test.test_roaring_bitmap");
        }
    }
}
