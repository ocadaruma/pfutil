package com.mayreh.pfutil.v4;

import lombok.Builder;
import lombok.Value;
import lombok.experimental.Accessors;

import static com.mayreh.pfutil.v4.Hllhdr.HEADER_BYTES_LEN;

/**
 * HLL Configurations
 * These configurations are originally defined as #define macros in hyperloglog.c
 */
@Value
@Builder
@Accessors(fluent = true)
public class Config {
    public static final Config DEFAULT = Config.builder().build();

    @Builder.Default
    int hllP = 14;

    @Builder.Default
    int hllBits = 6;

    @Builder.Default
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
