package com.mayreh.pfutil.v4;

import java.nio.ByteBuffer;

/**
 * Provides Redis v4 compatible HLL features
 * This class is NOT thread-safe since every operation possibly mutates underlying byte array as in original C-implementation.
 */
public class HllV4 {
    private Hllhdr hllhdr;

    public HllV4(Config config, byte[] hdrBytes) {
        ByteBuffer hdrBuffer = ByteBuffer.wrap(hdrBytes);
        hllhdr = Hllhdr.fromRepr(config, hdrBuffer);

        if (hllhdr.isValidHllObject()) {
            throw new IllegalArgumentException("Invalid HLL binary");
        }
    }

    /**
     * Cardinality of the single HLL
     */
    public long pfCount() {
        if (hllhdr.getHeader().isValidCache()) {
            return hllhdr.getHeader().getCardinality();
        } else {
            Hllhdr.HllCountResult result = hllhdr.hllCount();
            if (!result.isValid()) {
                throw new IllegalStateException("hllCount result is invalid");
            }
            return result.getCount();
        }
    }

    public int pfAdd(String element) {
        throw new UnsupportedOperationException("to be implemented");
    }

    public byte[] dump() {
        throw new UnsupportedOperationException("to be implemented");
    }
}
