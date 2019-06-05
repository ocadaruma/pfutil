package com.mayreh.pfutil.v4;

import com.mayreh.pfutil.Hll;
import com.mayreh.pfutil.RedisVersion;

import java.nio.ByteBuffer;

/**
 * Provides Redis v4 compatible HLL features
 */
public class HllV4 implements Hll {
    private ByteBuffer hdrBuffer;

    public HllV4(byte[] hdrBytes) {
        this.hdrBuffer = ByteBuffer.wrap(hdrBytes);
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
