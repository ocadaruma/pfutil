package com.mayreh.pfutil;

/**
 * An interface that provides HLL features
 */
public interface HLL {
    RedisVersion version();

    long pfCount();

    int pfAdd(String element);

    byte[] dump();

    static HLL of(RedisVersion version, byte[] hdr) {
        switch (version) {
            case V4:
                return new HLLv4(hdr);
            default:
                throw new IllegalArgumentException("invalid version");
        }
    }
}
