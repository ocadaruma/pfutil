package com.mayreh.pfutil.v4;

import lombok.Builder;
import lombok.Getter;
import lombok.Value;

import java.nio.ByteBuffer;

/**
 * Java clone of https://github.com/antirez/redis/blob/4.0.10/src/hyperloglog.c
 */
public class Hllhdr {
    private final Config config;
    private final ByteBuffer buffer;
    @Getter
    private final Header header;

    private static final int HEADER_BYTES_LEN = 16;

    enum Encoding {
        HLL_DENSE,
        HLL_SPARSE,
    }

    @Value
    @Builder
    public static class Config {
        int hllSparseMaxBytes = 3000;
        int hllP = 14;
        int hllBits = 6;

        public int hllRegisters() {
            return 1 << hllP;
        }

        public int hllDenseSize() {
            return HEADER_BYTES_LEN + ((hllRegisters() * hllBits + 7) / 8);
        }

        public int hllRegisterMax() {
            return (1 << hllBits) - 1;
        }
    }

    @Value
    public static class HllCountResult {
        boolean valid;
        long count;
    }

    @Value
    public static class SumResult {
        double ez;
        double E;
    }

    public Hllhdr(Config config, ByteBuffer buffer) {
        this.config = config;
        this.buffer = buffer;
        this.header = Header.scan(buffer);
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
                cardinality = (long)card[0] & 0xFFL;
                cardinality |= ((long)card[1] & 0xFFL) << 8;
                cardinality |= ((long)card[2] & 0xFFL) << 16;
                cardinality |= ((long)card[3] & 0xFFL) << 24;
                cardinality |= ((long)card[4] & 0xFFL) << 32;
                cardinality |= ((long)card[5] & 0xFFL) << 40;
                cardinality |= ((long)card[6] & 0xFFL) << 48;
                cardinality |= ((long)card[7] & 0xFFL) << 56;
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
        // seek to start of data
        this.buffer.position(HEADER_BYTES_LEN);

        double m = config.hllRegisters();
        double alpha = 0.7213 / (1 + 1.079 / m);
        SumResult sum;

        if (this.header.encoding == Encoding.HLL_DENSE) {
            byte[] registers = new byte[this.buffer.remaining()];
            this.buffer.get(registers);

            Dense dense = new Dense(config, registers);
            sum = dense.denseSum();
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

//    private double hllDenseSum() {
//        byte[] registers = new byte[buffer.remaining()];
//        buffer.get(registers);
//
//        Dense dense = new Dense(config, registers);
//        Dense.SumResult sum = dense.denseSum();
//
//
//    }
}
