package com.mayreh.pfutil.v4;

import lombok.Value;

import java.nio.ByteBuffer;

public class Hllhdr {
    private final ByteBuffer buffer;
    private Header header;

    private static final int HEADER_BYTES_LEN = 16;
    private static final int HLL_P = 14;
    private static final int HLL_REGISTERS = 1 << HLL_P;
    private static final int HLL_BITS = 6;
    private static final int HLL_DENSE_SIZE =
            HEADER_BYTES_LEN + ((HLL_REGISTERS * HLL_BITS + 7) / 8);

    enum Encoding {
        HLL_DENSE,
        HLL_SPARSE,
    }

    public Hllhdr(ByteBuffer buffer) {
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
                this.buffer.capacity() != HLL_DENSE_SIZE) {
            return false;
        }

        return true;
    }

    @Value
    public static class HllCountResult {
        boolean valid;
        long count;
    }
}
