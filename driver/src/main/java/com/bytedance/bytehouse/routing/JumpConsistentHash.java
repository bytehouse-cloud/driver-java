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

/**
 * This class is meant to be a Java implementation of the same logic from
 * <a href="https://code.byted.org/dp/ClickHouse/blob/cnch_dev_gz/dbms/src/Common/TableHash.h">Cnch TableHash</a>.
 * <br><br>
 * The implementation is borrowed from
 * <a href="https://github.com/ssedano/jump-consistent-hash">ssedano/jump-consistent-hash</a>
 * <br><br>
 * the consistent hashing alrogithm is from this <a href="https://arxiv.org/pdf/1406.2294.pdf">Google paper</a>
 */
@SuppressWarnings({"checkstyle:linelength", "PMD"})
public final class JumpConsistentHash {

    private static final long UNSIGNED_MASK = 0x7fffffffffffffffL;

    private static final long JUMP = 1L << 31;

    private static final long CONSTANT = Long.parseUnsignedLong("2862933555777941757");

    /**
     * How this table is generated:
     * <br>1. copy the content fromt the c++ class
     * <br>2. save it into a file called file2
     * <br>3. run bash command ~$ sed -E "s|0x(.{16})ULL|Long\.parseUnsignedLong(\"\1\", 16)|g" file2
     */
    private static final long[] CRCTAB64 = {
            Long.parseUnsignedLong("0000000000000000", 16), Long.parseUnsignedLong("7ad870c830358979", 16), Long.parseUnsignedLong("f5b0e190606b12f2", 16),
            Long.parseUnsignedLong("8f689158505e9b8b", 16), Long.parseUnsignedLong("c038e5739841b68f", 16), Long.parseUnsignedLong("bae095bba8743ff6", 16),
            Long.parseUnsignedLong("358804e3f82aa47d", 16), Long.parseUnsignedLong("4f50742bc81f2d04", 16), Long.parseUnsignedLong("ab28ecb46814fe75", 16),
            Long.parseUnsignedLong("d1f09c7c5821770c", 16), Long.parseUnsignedLong("5e980d24087fec87", 16), Long.parseUnsignedLong("24407dec384a65fe", 16),
            Long.parseUnsignedLong("6b1009c7f05548fa", 16), Long.parseUnsignedLong("11c8790fc060c183", 16), Long.parseUnsignedLong("9ea0e857903e5a08", 16),
            Long.parseUnsignedLong("e478989fa00bd371", 16), Long.parseUnsignedLong("7d08ff3b88be6f81", 16), Long.parseUnsignedLong("07d08ff3b88be6f8", 16),
            Long.parseUnsignedLong("88b81eabe8d57d73", 16), Long.parseUnsignedLong("f2606e63d8e0f40a", 16), Long.parseUnsignedLong("bd301a4810ffd90e", 16),
            Long.parseUnsignedLong("c7e86a8020ca5077", 16), Long.parseUnsignedLong("4880fbd87094cbfc", 16), Long.parseUnsignedLong("32588b1040a14285", 16),
            Long.parseUnsignedLong("d620138fe0aa91f4", 16), Long.parseUnsignedLong("acf86347d09f188d", 16), Long.parseUnsignedLong("2390f21f80c18306", 16),
            Long.parseUnsignedLong("594882d7b0f40a7f", 16), Long.parseUnsignedLong("1618f6fc78eb277b", 16), Long.parseUnsignedLong("6cc0863448deae02", 16),
            Long.parseUnsignedLong("e3a8176c18803589", 16), Long.parseUnsignedLong("997067a428b5bcf0", 16), Long.parseUnsignedLong("fa11fe77117cdf02", 16),
            Long.parseUnsignedLong("80c98ebf2149567b", 16), Long.parseUnsignedLong("0fa11fe77117cdf0", 16), Long.parseUnsignedLong("75796f2f41224489", 16),
            Long.parseUnsignedLong("3a291b04893d698d", 16), Long.parseUnsignedLong("40f16bccb908e0f4", 16), Long.parseUnsignedLong("cf99fa94e9567b7f", 16),
            Long.parseUnsignedLong("b5418a5cd963f206", 16), Long.parseUnsignedLong("513912c379682177", 16), Long.parseUnsignedLong("2be1620b495da80e", 16),
            Long.parseUnsignedLong("a489f35319033385", 16), Long.parseUnsignedLong("de51839b2936bafc", 16), Long.parseUnsignedLong("9101f7b0e12997f8", 16),
            Long.parseUnsignedLong("ebd98778d11c1e81", 16), Long.parseUnsignedLong("64b116208142850a", 16), Long.parseUnsignedLong("1e6966e8b1770c73", 16),
            Long.parseUnsignedLong("8719014c99c2b083", 16), Long.parseUnsignedLong("fdc17184a9f739fa", 16), Long.parseUnsignedLong("72a9e0dcf9a9a271", 16),
            Long.parseUnsignedLong("08719014c99c2b08", 16), Long.parseUnsignedLong("4721e43f0183060c", 16), Long.parseUnsignedLong("3df994f731b68f75", 16),
            Long.parseUnsignedLong("b29105af61e814fe", 16), Long.parseUnsignedLong("c849756751dd9d87", 16), Long.parseUnsignedLong("2c31edf8f1d64ef6", 16),
            Long.parseUnsignedLong("56e99d30c1e3c78f", 16), Long.parseUnsignedLong("d9810c6891bd5c04", 16), Long.parseUnsignedLong("a3597ca0a188d57d", 16),
            Long.parseUnsignedLong("ec09088b6997f879", 16), Long.parseUnsignedLong("96d1784359a27100", 16), Long.parseUnsignedLong("19b9e91b09fcea8b", 16),
            Long.parseUnsignedLong("636199d339c963f2", 16), Long.parseUnsignedLong("df7adabd7a6e2d6f", 16), Long.parseUnsignedLong("a5a2aa754a5ba416", 16),
            Long.parseUnsignedLong("2aca3b2d1a053f9d", 16), Long.parseUnsignedLong("50124be52a30b6e4", 16), Long.parseUnsignedLong("1f423fcee22f9be0", 16),
            Long.parseUnsignedLong("659a4f06d21a1299", 16), Long.parseUnsignedLong("eaf2de5e82448912", 16), Long.parseUnsignedLong("902aae96b271006b", 16),
            Long.parseUnsignedLong("74523609127ad31a", 16), Long.parseUnsignedLong("0e8a46c1224f5a63", 16), Long.parseUnsignedLong("81e2d7997211c1e8", 16),
            Long.parseUnsignedLong("fb3aa75142244891", 16), Long.parseUnsignedLong("b46ad37a8a3b6595", 16), Long.parseUnsignedLong("ceb2a3b2ba0eecec", 16),
            Long.parseUnsignedLong("41da32eaea507767", 16), Long.parseUnsignedLong("3b024222da65fe1e", 16), Long.parseUnsignedLong("a2722586f2d042ee", 16),
            Long.parseUnsignedLong("d8aa554ec2e5cb97", 16), Long.parseUnsignedLong("57c2c41692bb501c", 16), Long.parseUnsignedLong("2d1ab4dea28ed965", 16),
            Long.parseUnsignedLong("624ac0f56a91f461", 16), Long.parseUnsignedLong("1892b03d5aa47d18", 16), Long.parseUnsignedLong("97fa21650afae693", 16),
            Long.parseUnsignedLong("ed2251ad3acf6fea", 16), Long.parseUnsignedLong("095ac9329ac4bc9b", 16), Long.parseUnsignedLong("7382b9faaaf135e2", 16),
            Long.parseUnsignedLong("fcea28a2faafae69", 16), Long.parseUnsignedLong("8632586aca9a2710", 16), Long.parseUnsignedLong("c9622c4102850a14", 16),
            Long.parseUnsignedLong("b3ba5c8932b0836d", 16), Long.parseUnsignedLong("3cd2cdd162ee18e6", 16), Long.parseUnsignedLong("460abd1952db919f", 16),
            Long.parseUnsignedLong("256b24ca6b12f26d", 16), Long.parseUnsignedLong("5fb354025b277b14", 16), Long.parseUnsignedLong("d0dbc55a0b79e09f", 16),
            Long.parseUnsignedLong("aa03b5923b4c69e6", 16), Long.parseUnsignedLong("e553c1b9f35344e2", 16), Long.parseUnsignedLong("9f8bb171c366cd9b", 16),
            Long.parseUnsignedLong("10e3202993385610", 16), Long.parseUnsignedLong("6a3b50e1a30ddf69", 16), Long.parseUnsignedLong("8e43c87e03060c18", 16),
            Long.parseUnsignedLong("f49bb8b633338561", 16), Long.parseUnsignedLong("7bf329ee636d1eea", 16), Long.parseUnsignedLong("012b592653589793", 16),
            Long.parseUnsignedLong("4e7b2d0d9b47ba97", 16), Long.parseUnsignedLong("34a35dc5ab7233ee", 16), Long.parseUnsignedLong("bbcbcc9dfb2ca865", 16),
            Long.parseUnsignedLong("c113bc55cb19211c", 16), Long.parseUnsignedLong("5863dbf1e3ac9dec", 16), Long.parseUnsignedLong("22bbab39d3991495", 16),
            Long.parseUnsignedLong("add33a6183c78f1e", 16), Long.parseUnsignedLong("d70b4aa9b3f20667", 16), Long.parseUnsignedLong("985b3e827bed2b63", 16),
            Long.parseUnsignedLong("e2834e4a4bd8a21a", 16), Long.parseUnsignedLong("6debdf121b863991", 16), Long.parseUnsignedLong("1733afda2bb3b0e8", 16),
            Long.parseUnsignedLong("f34b37458bb86399", 16), Long.parseUnsignedLong("8993478dbb8deae0", 16), Long.parseUnsignedLong("06fbd6d5ebd3716b", 16),
            Long.parseUnsignedLong("7c23a61ddbe6f812", 16), Long.parseUnsignedLong("3373d23613f9d516", 16), Long.parseUnsignedLong("49aba2fe23cc5c6f", 16),
            Long.parseUnsignedLong("c6c333a67392c7e4", 16), Long.parseUnsignedLong("bc1b436e43a74e9d", 16), Long.parseUnsignedLong("95ac9329ac4bc9b5", 16),
            Long.parseUnsignedLong("ef74e3e19c7e40cc", 16), Long.parseUnsignedLong("601c72b9cc20db47", 16), Long.parseUnsignedLong("1ac40271fc15523e", 16),
            Long.parseUnsignedLong("5594765a340a7f3a", 16), Long.parseUnsignedLong("2f4c0692043ff643", 16), Long.parseUnsignedLong("a02497ca54616dc8", 16),
            Long.parseUnsignedLong("dafce7026454e4b1", 16), Long.parseUnsignedLong("3e847f9dc45f37c0", 16), Long.parseUnsignedLong("445c0f55f46abeb9", 16),
            Long.parseUnsignedLong("cb349e0da4342532", 16), Long.parseUnsignedLong("b1eceec59401ac4b", 16), Long.parseUnsignedLong("febc9aee5c1e814f", 16),
            Long.parseUnsignedLong("8464ea266c2b0836", 16), Long.parseUnsignedLong("0b0c7b7e3c7593bd", 16), Long.parseUnsignedLong("71d40bb60c401ac4", 16),
            Long.parseUnsignedLong("e8a46c1224f5a634", 16), Long.parseUnsignedLong("927c1cda14c02f4d", 16), Long.parseUnsignedLong("1d148d82449eb4c6", 16),
            Long.parseUnsignedLong("67ccfd4a74ab3dbf", 16), Long.parseUnsignedLong("289c8961bcb410bb", 16), Long.parseUnsignedLong("5244f9a98c8199c2", 16),
            Long.parseUnsignedLong("dd2c68f1dcdf0249", 16), Long.parseUnsignedLong("a7f41839ecea8b30", 16), Long.parseUnsignedLong("438c80a64ce15841", 16),
            Long.parseUnsignedLong("3954f06e7cd4d138", 16), Long.parseUnsignedLong("b63c61362c8a4ab3", 16), Long.parseUnsignedLong("cce411fe1cbfc3ca", 16),
            Long.parseUnsignedLong("83b465d5d4a0eece", 16), Long.parseUnsignedLong("f96c151de49567b7", 16), Long.parseUnsignedLong("76048445b4cbfc3c", 16),
            Long.parseUnsignedLong("0cdcf48d84fe7545", 16), Long.parseUnsignedLong("6fbd6d5ebd3716b7", 16), Long.parseUnsignedLong("15651d968d029fce", 16),
            Long.parseUnsignedLong("9a0d8ccedd5c0445", 16), Long.parseUnsignedLong("e0d5fc06ed698d3c", 16), Long.parseUnsignedLong("af85882d2576a038", 16),
            Long.parseUnsignedLong("d55df8e515432941", 16), Long.parseUnsignedLong("5a3569bd451db2ca", 16), Long.parseUnsignedLong("20ed197575283bb3", 16),
            Long.parseUnsignedLong("c49581ead523e8c2", 16), Long.parseUnsignedLong("be4df122e51661bb", 16), Long.parseUnsignedLong("3125607ab548fa30", 16),
            Long.parseUnsignedLong("4bfd10b2857d7349", 16), Long.parseUnsignedLong("04ad64994d625e4d", 16), Long.parseUnsignedLong("7e7514517d57d734", 16),
            Long.parseUnsignedLong("f11d85092d094cbf", 16), Long.parseUnsignedLong("8bc5f5c11d3cc5c6", 16), Long.parseUnsignedLong("12b5926535897936", 16),
            Long.parseUnsignedLong("686de2ad05bcf04f", 16), Long.parseUnsignedLong("e70573f555e26bc4", 16), Long.parseUnsignedLong("9ddd033d65d7e2bd", 16),
            Long.parseUnsignedLong("d28d7716adc8cfb9", 16), Long.parseUnsignedLong("a85507de9dfd46c0", 16), Long.parseUnsignedLong("273d9686cda3dd4b", 16),
            Long.parseUnsignedLong("5de5e64efd965432", 16), Long.parseUnsignedLong("b99d7ed15d9d8743", 16), Long.parseUnsignedLong("c3450e196da80e3a", 16),
            Long.parseUnsignedLong("4c2d9f413df695b1", 16), Long.parseUnsignedLong("36f5ef890dc31cc8", 16), Long.parseUnsignedLong("79a59ba2c5dc31cc", 16),
            Long.parseUnsignedLong("037deb6af5e9b8b5", 16), Long.parseUnsignedLong("8c157a32a5b7233e", 16), Long.parseUnsignedLong("f6cd0afa9582aa47", 16),
            Long.parseUnsignedLong("4ad64994d625e4da", 16), Long.parseUnsignedLong("300e395ce6106da3", 16), Long.parseUnsignedLong("bf66a804b64ef628", 16),
            Long.parseUnsignedLong("c5bed8cc867b7f51", 16), Long.parseUnsignedLong("8aeeace74e645255", 16), Long.parseUnsignedLong("f036dc2f7e51db2c", 16),
            Long.parseUnsignedLong("7f5e4d772e0f40a7", 16), Long.parseUnsignedLong("05863dbf1e3ac9de", 16), Long.parseUnsignedLong("e1fea520be311aaf", 16),
            Long.parseUnsignedLong("9b26d5e88e0493d6", 16), Long.parseUnsignedLong("144e44b0de5a085d", 16), Long.parseUnsignedLong("6e963478ee6f8124", 16),
            Long.parseUnsignedLong("21c640532670ac20", 16), Long.parseUnsignedLong("5b1e309b16452559", 16), Long.parseUnsignedLong("d476a1c3461bbed2", 16),
            Long.parseUnsignedLong("aeaed10b762e37ab", 16), Long.parseUnsignedLong("37deb6af5e9b8b5b", 16), Long.parseUnsignedLong("4d06c6676eae0222", 16),
            Long.parseUnsignedLong("c26e573f3ef099a9", 16), Long.parseUnsignedLong("b8b627f70ec510d0", 16), Long.parseUnsignedLong("f7e653dcc6da3dd4", 16),
            Long.parseUnsignedLong("8d3e2314f6efb4ad", 16), Long.parseUnsignedLong("0256b24ca6b12f26", 16), Long.parseUnsignedLong("788ec2849684a65f", 16),
            Long.parseUnsignedLong("9cf65a1b368f752e", 16), Long.parseUnsignedLong("e62e2ad306bafc57", 16), Long.parseUnsignedLong("6946bb8b56e467dc", 16),
            Long.parseUnsignedLong("139ecb4366d1eea5", 16), Long.parseUnsignedLong("5ccebf68aecec3a1", 16), Long.parseUnsignedLong("2616cfa09efb4ad8", 16),
            Long.parseUnsignedLong("a97e5ef8cea5d153", 16), Long.parseUnsignedLong("d3a62e30fe90582a", 16), Long.parseUnsignedLong("b0c7b7e3c7593bd8", 16),
            Long.parseUnsignedLong("ca1fc72bf76cb2a1", 16), Long.parseUnsignedLong("45775673a732292a", 16), Long.parseUnsignedLong("3faf26bb9707a053", 16),
            Long.parseUnsignedLong("70ff52905f188d57", 16), Long.parseUnsignedLong("0a2722586f2d042e", 16), Long.parseUnsignedLong("854fb3003f739fa5", 16),
            Long.parseUnsignedLong("ff97c3c80f4616dc", 16), Long.parseUnsignedLong("1bef5b57af4dc5ad", 16), Long.parseUnsignedLong("61372b9f9f784cd4", 16),
            Long.parseUnsignedLong("ee5fbac7cf26d75f", 16), Long.parseUnsignedLong("9487ca0fff135e26", 16), Long.parseUnsignedLong("dbd7be24370c7322", 16),
            Long.parseUnsignedLong("a10fceec0739fa5b", 16), Long.parseUnsignedLong("2e675fb4576761d0", 16), Long.parseUnsignedLong("54bf2f7c6752e8a9", 16),
            Long.parseUnsignedLong("cdcf48d84fe75459", 16), Long.parseUnsignedLong("b71738107fd2dd20", 16), Long.parseUnsignedLong("387fa9482f8c46ab", 16),
            Long.parseUnsignedLong("42a7d9801fb9cfd2", 16), Long.parseUnsignedLong("0df7adabd7a6e2d6", 16), Long.parseUnsignedLong("772fdd63e7936baf", 16),
            Long.parseUnsignedLong("f8474c3bb7cdf024", 16), Long.parseUnsignedLong("829f3cf387f8795d", 16), Long.parseUnsignedLong("66e7a46c27f3aa2c", 16),
            Long.parseUnsignedLong("1c3fd4a417c62355", 16), Long.parseUnsignedLong("935745fc4798b8de", 16), Long.parseUnsignedLong("e98f353477ad31a7", 16),
            Long.parseUnsignedLong("a6df411fbfb21ca3", 16), Long.parseUnsignedLong("dc0731d78f8795da", 16), Long.parseUnsignedLong("536fa08fdfd90e51", 16),
            Long.parseUnsignedLong("29b7d047efec8728", 16)
    };

    private JumpConsistentHash() {
        throw new IllegalStateException("No creating of "
                + JumpConsistentHash.class.getName() + " !");
    }

    /**
     * generate unsigned long value from char[].
     *
     * @param buffer char array input
     * @return unsigned long
     */
    static long fioCrc64(final char[] buffer) {
        long crc = 0;
        int ptr = 0;

        while (ptr < buffer.length) {
            crc = CRCTAB64[(int) ((crc ^ (buffer[ptr++])) & 0xff)] ^ (crc >>> 8);
        }

        return crc;
    }

    /**
     * Accepts "a 64-bit key and the number of buckets. It outputs a number in
     * the range [0, buckets]."
     *
     * @param key        key to store
     * @param numBuckets number of available buckets
     * @return the hash of the key
     * @throws IllegalArgumentException if buckets is less than 0
     */
    static int jumpConsistentHash(final long key, final int numBuckets) {
        checkBuckets(numBuckets);
        long k = key;
        long b = -1;
        long j = 0;

        while (j < numBuckets) {
            b = j;
            k = k * CONSTANT + 1L;

            j = (long) ((b + 1L) * (JUMP / toDouble((k >>> 33) + 1L)));
        }
        return (int) b;
    }

    private static void checkBuckets(final int buckets) {
        if (buckets < 0) {
            throw new IllegalArgumentException("Buckets cannot be less than 0");
        }
    }

    private static double toDouble(final long n) {
        double d = n & UNSIGNED_MASK;
        if (n < 0) {
            d += 0x1.0p63;
        }
        return d;
    }

    /**
     * Consistent hashing of the input string onto the number of nodes.
     *
     * @param s          input string
     * @param numBuckets number of buckets
     * @return bucket number
     */
    public static int consistentHashForString(final String s, final int numBuckets) {
        return jumpConsistentHash(fioCrc64(s.toCharArray()), numBuckets);
    }
}
