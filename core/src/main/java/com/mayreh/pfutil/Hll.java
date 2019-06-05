package com.mayreh.pfutil;

import com.mayreh.pfutil.v4.HllV4;
import com.mayreh.pfutil.v4.Hllhdr;

/**
 * An interface that provides HLL features
 */
public interface Hll {
    RedisVersion version();

    long pfCount();

    int pfAdd(String element);

    byte[] dump();

    static Hll loadV4(Hllhdr.Config config, byte[] hdr) {
        return new HllV4(config, hdr);
    }

    static Hll loadV4(byte[] hdr) {
        return new HllV4(Hllhdr.Config.DEFAULT, hdr);
    }
}
