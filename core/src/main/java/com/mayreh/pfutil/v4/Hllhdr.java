package com.mayreh.pfutil.v4;

import com.mayreh.pfutil.HllUtil;
import lombok.Builder;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Value;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Java clone of https://github.com/antirez/redis/blob/4.0.10/src/hyperloglog.c
 */
public class Hllhdr {
    private final Config config;
    private final ByteBuffer buffer;
    @Getter
    private final Header header;

    public static final int HEADER_BYTES_LEN = 16;

    @RequiredArgsConstructor
    enum Encoding {
        HLL_DENSE((byte)0),
        HLL_SPARSE((byte)1);

        public final byte value;
    }

    @Value
    @Builder
    public static class Config {
        public static final Config DEFAULT = Config.builder().build();

        int hllSparseMaxBytes = 3000;
        int hllP = 14;
        int hllBits = 6;
        int hllSparseZeroMaxLen = 64;
        int hllSparseXZeroMaxLen = 16384;

        public int hllRegisters() {
            return 1 << hllP;
        }

        public int hllDenseSize() {
            return HEADER_BYTES_LEN + ((hllRegisters() * hllBits + 7) / 8);
        }

        public int hllRegisterMax() {
            return (1 << hllBits) - 1;
        }

        public int hllPMask() {
            return hllRegisters() - 1;
        }
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
        int len; // the length of the pattern 000..1 of the element hash
        int reg; // the register index the element hashes to
    }

    private Hllhdr(Config config, ByteBuffer buffer) {
        this.config = config;
        this.buffer = buffer;
        this.header = Header.scan(buffer);
    }

    /**
     * Instantiate Hllhdr from existing Redis v4 representation binary
     */
    public static Hllhdr fromRepr(Config config, ByteBuffer buffer) {
        return new Hllhdr(config, buffer);
    }

    /**
     * Instantiate empty Hllhdr
     */
    public static Hllhdr create(Config config) {
        int sparseLen = HEADER_BYTES_LEN + (
                ((config.hllRegisters() + (config.hllSparseXZeroMaxLen - 1)) / config.hllSparseXZeroMaxLen) * 2
        );

        ByteBuffer buffer = ByteBuffer.allocate(sparseLen);
        Sparse.initialize(config, buffer);

        buffer.rewind();
        return new Hllhdr(config, buffer);
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

    @Value
    public static class Header {
        private static final byte[] magic = new byte[]{'H', 'Y', 'L', 'L'};

        Encoding encoding;
        boolean validCache;
        long cardinality;

        /**
         * Attempt to scan header and increment buffer position
         * Returns header if succeeded otherwise null
         */
        public static Header scan(ByteBuffer buffer) {
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

    public int hllAdd(byte[] element) {
        switch (this.header.encoding) {
            case HLL_DENSE:
                return new Dense(config, buffer).denseAdd(element);
            case HLL_SPARSE:
                throw new UnsupportedOperationException("to be implemented");
            default:
                return -1;
        }
    }

    /**
     * Returns copy of current HLL representation
     */
    public byte[] dump() {
        buffer.rewind();
        byte[] src = buffer.array();
        return Arrays.copyOf(src, src.length);
    }
}
