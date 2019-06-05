package com.mayreh.pfutil.v4;

import com.mayreh.pfutil.HllUtil;
import lombok.RequiredArgsConstructor;
import lombok.Value;

@RequiredArgsConstructor
class Sparse {
    private final Hllhdr.Config config;
    private final byte[] sparseBytes;

    private static final int HLL_SPARSE_XZERO_BIT = 0x40;

    @Value
    public static class SparseSumResult {
        boolean valid;
        Hllhdr.SumResult sum;
    }

    private static boolean sparseIsZero(byte b) {
        return (((int)b & 0xFF) & 0xc0) == 0;
    }

    private static boolean sparseIsXZero(byte b) {
        return (((int)b & 0xFF) & 0xc0) == HLL_SPARSE_XZERO_BIT;
    }

    private static int sparseZeroLen(byte b) {
        return (((int)b & 0xFF) & 0x3f) + 1;
    }

    private static int sparseXZeroLen(byte b, byte nextB) {
        return ((((int)b & 0xFF) & 0x3f) << 8) | ((int)nextB & 0xFF) + 1;
    }

    private static int sparseValValue(byte b) {
        return ((((int)b & 0xFF) >> 2) & 0x1f) + 1;
    }

    private static int sparseValLen(byte b) {
        return (((int)b & 0xFF) & 0x3) + 1;
    }

    public SparseSumResult sparseSum() {
        double E = 0.0;
        int ez = 0;
        int idx = 0;

        int p = 0;
        while (p < sparseBytes.length) {
            byte b = sparseBytes[p];

            if (sparseIsZero(b)) {
                int runlen = sparseZeroLen(b);
                idx += runlen;
                ez += runlen;
                p++;
            } else if (sparseIsXZero(b)) {
                int runlen = sparseXZeroLen(b, sparseBytes[p + 1]);
                idx += runlen;
                ez += runlen;
                p += 2;
            } else {
                int runlen = sparseValLen(b);
                int regVal = sparseValValue(b);
                idx += runlen;
                E += HllUtil.pow2(-regVal) * runlen;
                p++;
            }
        }

        if (idx != config.hllRegisters()) {
            return new SparseSumResult(false, null);
        }
        E += ez;
        return new SparseSumResult(true, new Hllhdr.SumResult(ez, E));
    }
}
