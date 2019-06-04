package com.mayreh.pfutil;

/**
 * Provides Redis v4 compatible HLL features
 */
public class HLLv4 implements HLL {
    private byte[] hdr;

    public HLLv4(byte[] hdr) {
        this.hdr = hdr;
    }

    @Override
    public RedisVersion version() {
        return RedisVersion.V4;
    }

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
