package com.mayreh.pfutil;

import com.mayreh.pfutil.v4.HllV4;

/**
 * An interface that provides HLL features
 */
public interface Hll {
    RedisVersion version();

    long pfCount();

    int pfAdd(String element);

    byte[] dump();

    static Hll load(RedisVersion version, byte[] hdr) {
        switch (version) {
            case V4:
                return new HllV4(hdr);
            default:
                throw new IllegalArgumentException("invalid version");
        }
    }
}
