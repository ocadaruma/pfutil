package com.mayreh.pfutil.v4;

import com.mayreh.pfutil.Hll;
import com.mayreh.pfutil.RedisVersion;

import java.nio.ByteBuffer;

/**
 * Provides Redis v4 compatible HLL features
 * This class is NOT thread-safe since every operation possibly mutates underlying byte array as in original C-implementation.
 */
public class HllV4 implements Hll {
    private Hllhdr hllhdr;

    public HllV4(byte[] hdrBytes) {
        ByteBuffer hdrBuffer = ByteBuffer.wrap(hdrBytes);
        Hllhdr.Config config = Hllhdr.Config.builder().build();
        hllhdr = new Hllhdr(config, hdrBuffer);

        if (hllhdr.isValidHllObject()) {
            throw new IllegalArgumentException("Invalid HLL binary");
        }
    }

    @Override
    public RedisVersion version() {
        return RedisVersion.V4;
    }

    /**
     * Cardinality of the single HLL
     */
    @Override
    public long pfCount() {
        if (hllhdr.getHeader().isValidCache()) {
            return hllhdr.getHeader().getCardinality();
        }
        throw new UnsupportedOperationException("to be implemented");
    }

    @Override
    public int pfAdd(String element) {
        throw new UnsupportedOperationException("to be implemented");
    }

    @Override
    public byte[] dump() {
        throw new UnsupportedOperationException("to be implemented");
    }
}
