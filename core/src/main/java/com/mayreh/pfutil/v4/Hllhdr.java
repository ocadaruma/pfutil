package com.mayreh.pfutil.v4;

import com.mayreh.pfutil.HllUtil;
import lombok.*;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Java clone of https://github.com/antirez/redis/blob/4.0.10/src/hyperloglog.c
 */
class Hllhdr {
    private final Config config;
    private ByteBuffer buffer;
    @Getter
    private Header header;

    public static final int HEADER_BYTES_LEN = 16;

    @RequiredArgsConstructor
    enum Encoding {
        HLL_DENSE((byte)0),
        HLL_SPARSE((byte)1);

        public final byte value;
    }

    @Value
    public static class HllCountResult {
        boolean valid;
        long count;
    }

    @Value
    public static class SumResult {
        int ez;
        double E;
    }

    @Value
    public static class PatLenResult {
        int len; // the length of the pattern 000..1 in the element hash
        int reg; // the register index the element hashes to
    }

    public Hllhdr(Config config, ByteBuffer buffer) {
        this.config = config;
        this.buffer = buffer;
        this.header = Header.scan(buffer);
    }

    public Hllhdr(Config config) {
        int sparseLen = HEADER_BYTES_LEN + (
                ((config.hllRegisters() + (config.hllSparseXZeroMaxLen() - 1)) / config.hllSparseXZeroMaxLen()) * 2
        );

        ByteBuffer buffer = ByteBuffer.allocate(sparseLen);
        Sparse.initialize(config, buffer);

        buffer.rewind();

        this.config = config;
        this.buffer = buffer;
        this.header = Header.scan(buffer);
    }

    static PatLenResult hllPatLen(Config config, byte[] element) {
        long hash = HllUtil.murmurHash64A(element, 0xadc83b19);
        long index = hash & config.hllPMask();
        hash |= (1L << 63);

        long bit = config.hllRegisters();

        int count = 1;
        while ((hash & bit) == 0) {
            count++;
            bit <<= 1;
        }
        return new PatLenResult(count, (int)index);
    }

    void invalidateCache() {
        // set position to cache flag
        buffer.position(15).mark();
        byte b = buffer.get();

        buffer.reset();
        buffer.put((byte)(b | (1 << 7)));

        header.setValidCache(false);
    }

    void setCache(long count) {
        // set position to the beginning of Cardin.
        buffer.position(8).mark();

        buffer.put((byte)(count & 0xff));
        buffer.put((byte)((count >>> 8) & 0xff));
        buffer.put((byte)((count >>> 16) & 0xff));
        buffer.put((byte)((count >>> 24) & 0xff));
        buffer.put((byte)((count >>> 32) & 0xff));
        buffer.put((byte)((count >>> 40) & 0xff));
        buffer.put((byte)((count >>> 48) & 0xff));
        buffer.put((byte)((count >>> 56) & 0xff));

        header.setValidCache(true);
        header.setCardinality(count);
    }

    @Data
    @AllArgsConstructor
    public static class Header {
        private static final byte[] magic = new byte[]{'H', 'Y', 'L', 'L'};

        @Setter(AccessLevel.PRIVATE)
        Encoding encoding;

        @Setter(AccessLevel.PRIVATE)
        boolean validCache;

        @Setter(AccessLevel.PRIVATE)
        long cardinality;

        /**
         * Attempt to scan header and increment buffer position
         *
         * @return  header if succeeded otherwise null
         */
        public static Header scan(ByteBuffer buffer) {
            buffer.rewind();
            if (buffer.capacity() < HEADER_BYTES_LEN) {
                return null;
            }

            // parse magic
            for (byte b : magic) {
                if (buffer.get() != b) {
                    return null;
                }
            }

            // parse encoding
            Encoding encoding;
            switch (buffer.get()) {
                case 0:
                    encoding = Encoding.HLL_DENSE;
                    break;
                case 1:
                    encoding = Encoding.HLL_SPARSE;
                    break;
                default:
                    return null;
            }

            // skip N/U bytes
            buffer.position(buffer.position() + 3);

            // parse Cardin.
            byte[] card = new byte[8];
            for (int i = 0; i < 8; i++) {
                card[i] = buffer.get();
            }
            boolean isValidCache = (card[7] & (1<<7)) == 0;
            long cardinality = 0;
            if (isValidCache) {
                cardinality = (long)card[0] & 0xffL;
                cardinality |= ((long)card[1] & 0xffL) << 8;
                cardinality |= ((long)card[2] & 0xffL) << 16;
                cardinality |= ((long)card[3] & 0xffL) << 24;
                cardinality |= ((long)card[4] & 0xffL) << 32;
                cardinality |= ((long)card[5] & 0xffL) << 40;
                cardinality |= ((long)card[6] & 0xffL) << 48;
                cardinality |= ((long)card[7] & 0xffL) << 56;
            }

            return new Header(encoding, isValidCache, cardinality);
        }
    }

    public boolean isValidHllObject() {
        if (this.header == null) {
            return false;
        }

        // validate dense representation
        if (this.header.encoding == Encoding.HLL_DENSE &&
                this.buffer.capacity() != config.hllDenseSize()) {
            return false;
        }

        return true;
    }

    public HllCountResult hllCount() {
        double m = config.hllRegisters();
        double alpha = 0.7213 / (1 + 1.079 / m);
        SumResult sum;

        if (this.header.encoding == Encoding.HLL_DENSE) {
            Dense dense = new Dense(config, buffer);
            sum = dense.denseSum();
        } else if (this.header.encoding == Encoding.HLL_SPARSE) {
            Sparse sparse = new Sparse(config, this.buffer);
            Sparse.SparseSumResult result = sparse.sparseSum();
            if (!result.isValid()) {
                return new HllCountResult(false, 0);
            }
            sum = result.getSum();
        } else {
            throw new UnsupportedOperationException("to be implemented");
        }

        double ez = sum.ez;
        double zl = Math.log(ez + 1);
        double beta = -0.370393911 * ez +
                0.070471823 * zl +
                0.17393686 * Math.pow(zl, 2) +
                0.16339839 * Math.pow(zl, 3) +
                -0.09237745 * Math.pow(zl, 4) +
                0.03738027 * Math.pow(zl, 5) +
                -0.005384159 * Math.pow(zl, 6) +
                0.00042419 * Math.pow(zl, 7);

        double result = Math.round(alpha * m * (m - ez) * (1 / (sum.E + beta)));
        return new HllCountResult(true, (long)result);
    }

    /**
     * Add element to HLL.
     * <p>
     * NOTE: Unlike original Redis implementation, the representation always be promoted to
     * dense representation regardless of sparseness for simplification.
     * </p>
     */
    public int hllAdd(byte[] element) {
        switch (this.header.encoding) {
            case HLL_DENSE:
                return new Dense(config, buffer).denseAdd(element);
            case HLL_SPARSE:
                hllSparseToDense();
                return new Dense(config, buffer).denseAdd(element);
            default:
                return -1;
        }
    }

    /**
     * @return snapshot of current HLL representation
     */
    public byte[] dump() {
        buffer.rewind();
        byte[] src = buffer.array();
        return Arrays.copyOf(src, src.length);
    }

    /**
     * Promote sparse representation to dense one.
     * <p>
     * Underlying buffer will be replaced to newly allocated dense buffer.
     * </p>
     */
    void hllSparseToDense() {
        // nothing to do
        if (header.encoding == Encoding.HLL_DENSE) {
            return;
        }

        ByteBuffer denseBuffer = ByteBuffer.allocate(config.hllDenseSize());

        // copy header
        byte[] headerSection = new byte[HEADER_BYTES_LEN];
        buffer.rewind();
        buffer.get(headerSection);
        denseBuffer.put(headerSection);

        // skip magic and put encoding
        denseBuffer.position(4);
        denseBuffer.put(Encoding.HLL_DENSE.value);

        int idx = 0;
        while (buffer.hasRemaining()) {
            byte b = buffer.get();
            if (Sparse.sparseIsZero(b)) {
                int runlen = Sparse.sparseZeroLen(b);
                idx += runlen;
            } else if (Sparse.sparseIsXZero(b)) {
                int runlen = Sparse.sparseXZeroLen(b, buffer.get());
                idx += runlen;
            } else {
                int runlen = Sparse.sparseValLen(b);
                int regval = Sparse.sparseValValue(b);

                while(runlen-- > 0) {
                    Dense.setRegisterAt(config, denseBuffer, idx, regval);
                    idx++;
                }
            }
        }

        if (idx != config.hllRegisters()) {
            throw new RuntimeException("failed to promote to dense");
        }

        this.buffer = denseBuffer;
        this.header = Header.scan(denseBuffer);
    }

    /**
     * Merge given HLLs into this HLL
     * <p>
     * NOTE: Unlike original Redis implementation, the representation always be promoted to
     * dense representation regardless of current encoding for simplification.
     * </p>
     */
    void hllMerge(Hllhdr... others) {
        byte[] max = new byte[config.hllRegisters()];
        hllMergeRegisters(config, max, this);
        hllMergeRegisters(config, max, others);

        if (header.encoding == Encoding.HLL_SPARSE) {
            hllSparseToDense();
        }

        for (int i = 0; i < max.length; i++) {
            Dense.setRegisterAt(config, buffer, i, max[i]);
        }
    }

    /**
     * Merge registers of given HLLs then write to target array
     */
    private static void hllMergeRegisters(Config config, byte[] max, Hllhdr... hlls) {
        for (Hllhdr hll : hlls) {
            boolean isDense = hll.header.encoding == Encoding.HLL_DENSE;
            if (isDense) {
                for (int i = 0; i < max.length; i++) {
                    long val = Dense.getRegisterAt(config, hll.buffer, i);
                    if (val > (max[i] & 0xffL)) {
                        max[i] = (byte)val;
                    }
                }
            } else {
                hll.buffer.position(HEADER_BYTES_LEN);

                int idx = 0;
                while (hll.buffer.hasRemaining()) {
                    byte b = hll.buffer.get();
                    long runlen;
                    if (Sparse.sparseIsZero(b)) {
                        runlen = Sparse.sparseZeroLen(b);
                        idx += runlen;
                    } else if (Sparse.sparseIsXZero(b)) {
                        runlen = Sparse.sparseXZeroLen(b, hll.buffer.get());
                        idx += runlen;
                    } else {
                        runlen = Sparse.sparseValLen(b);
                        int regval = Sparse.sparseValValue(b);
                        while (runlen-- > 0) {
                            if (regval > (max[idx] & 0xff)) {
                                max[idx] = (byte)regval;
                            }
                            idx++;
                        }
                    }
                }
                if (idx != config.hllRegisters()) {
                    throw new RuntimeException("failed to merge");
                }
            }
        }
    }
}
