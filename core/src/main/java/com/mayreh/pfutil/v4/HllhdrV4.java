package com.mayreh.pfutil.v4;

import com.mayreh.pfutil.HllByteBuffer;
import com.mayreh.pfutil.HllEncoding;
import com.mayreh.pfutil.HllUtil;

/**
 * A HLL representation compatible with Redis 4 (LogLog-Beta)
 */
class HllhdrV4 extends HllByteBuffer {

    HllhdrV4() {
        super();
    }

    HllhdrV4(byte[] repr) {
        super(repr);
    }

    private static class PatLenResult {
        private final int len; // the length of the pattern 000..1 in the element hash
        private final int reg; // the register index the element hashes to

        private PatLenResult(int len, int reg) {
            this.len = len;
            this.reg = reg;
        }
    }

    static class CountResult {
        final long count;
        final boolean valid;

        CountResult(long count, boolean valid) {
            this.count = count;
            this.valid = valid;
        }
    }

    static class SumResult {
        final int ez;
        final double E;
        final boolean valid;

        SumResult(int ez, double e, boolean valid) {
            this.ez = ez;
            this.E = e;
            this.valid = valid;
        }
    }

    private PatLenResult hllPatLen(byte[] element) {
        long hash = HllUtil.murmurHash64A(element, 0xadc83b19);
        long index = hash & registerBitsMask();
        hash |= (1L << 63);

        long bit = registerSize();

        int count = 1;
        while ((hash & bit) == 0) {
            count++;
            bit <<= 1;
        }
        return new PatLenResult(count, (int)index);
    }

    private SumResult denseSum() {
        int ez = 0;
        double E = 0;
        for (int i = 0; i < registerSize(); i++) {
            long reg = denseGetRegister(i);
            if (reg == 0) {
                ez++;
            } else {
                E += HllUtil.pow2(-(int)reg);
            }
        }
        E += ez;
        return new SumResult(ez, E, true);
    }

    private SumResult sparseSum() {
        double E = 0.0;
        int ez = 0;
        int idx = 0;

        int p = headerLen();
        while (p < buffer.length) {
            if (sparseIsZero(buffer[p])) {
                int runlen = sparseZeroLen(buffer[p]);
                idx += runlen;
                ez += runlen;
                p++;
            } else if (sparseIsXZero(buffer[p])) {
                int runlen = sparseXZeroLen(buffer[p], buffer[p + 1]);
                idx += runlen;
                ez += runlen;
                p += 2;
            } else {
                int runlen = sparseValLen(buffer[p]);
                int regVal = sparseValValue(buffer[p]);
                idx += runlen;
                E += HllUtil.pow2(-regVal) * runlen;
                p++;
            }
        }

        if (idx != registerSize()) {
            return new SumResult(0, 0, false);
        }
        E += ez;
        return new SumResult(ez, E, true);
    }

    boolean hllAdd(byte[] element) {
        PatLenResult patLen = hllPatLen(element);

        int retVal = hllSet(patLen.reg, patLen.len);
        if (retVal > 0) {
            invalidateCache();
            return true;
        } else {
            return false;
        }
    }

    CountResult hllCount() {
        double m = registerSize();
        double alpha = 0.7213 / (1 + 1.079 / m);
        SumResult sum;

        byte encoding = buffer[magic().length];
        if (encoding == HllEncoding.DENSE.value) {
            sum = denseSum();
        } else if (encoding == HllEncoding.SPARSE.value) {
            sum = sparseSum();
            if (!sum.valid) {
                return new CountResult(0, false);
            }
        } else {
            throw new IllegalStateException("invalid encoding : " + encoding);
        }

        double ez = sum.ez;
        double zl = Math.log(ez + 1);
        double beta = -0.370393911 * ez +
                0.070471823 * zl +
                0.17393686 * Math.pow(zl, 2) +
                0.16339839 * Math.pow(zl, 3) +
                -0.09237745 * Math.pow(zl, 4) +
                0.03738027 * Math.pow(zl, 5) +
                -0.005384159 * Math.pow(zl, 6) +
                0.00042419 * Math.pow(zl, 7);

        double result = Math.round(alpha * m * (m - ez) * (1 / (sum.E + beta)));
        return new CountResult((long)result, true);
    }
}
