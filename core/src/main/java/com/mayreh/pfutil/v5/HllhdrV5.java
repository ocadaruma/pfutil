package com.mayreh.pfutil.v5;

import com.mayreh.pfutil.HllByteBuffer;
import com.mayreh.pfutil.HllEncoding;
import com.mayreh.pfutil.HllUtil;

/**
 * A HLL representation compatible with Redis 5 based on arXiv:1702.01284
 */
class HllhdrV5 extends HllByteBuffer {
    private static final int HLL_Q;
    static {
        HLL_Q = 64 - registerBits();
    }

    // constant for 0.5/ln(2)
    private static final double HLL_ALPHA_INF = 0.721347520444481703680;

    HllhdrV5() {
        super();
    }

    HllhdrV5(byte[] repr) {
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

    private PatLenResult hllPatLen(byte[] element) {
        long hash = HllUtil.murmurHash64A(element, 0xadc83b19);
        long index = hash & registerBitsMask();
        hash >>>= registerBits();
        hash |= (1L << HLL_Q);

        long bit = 1L;
        int count = 1;
        while ((hash & bit) == 0) {
            count++;
            bit <<= 1;
        }
        return new PatLenResult(count, (int)index);
    }

    private double tau(double x) {
        if (x == 0.0 ||  x == 1.0) {
            return 0.0;
        }

        double zPrime;
        double y = 1.0;
        double z = 1 - x;
        do {
            x = Math.sqrt(x);
            zPrime = z;
            y *= 0.5;
            z -= Math.pow(1 - x, 2) * y;
        } while (zPrime != z);

        return z / 3;
    }

    private double sigma(double x) {
        if (x == 1.0) {
            return Double.POSITIVE_INFINITY;
        }

        double zPrime;
        double y = 1;
        double z = x;
        do {
            x *= x;
            zPrime = z;
            z += x * y;
            y += y;
        } while (zPrime != z);

        return z;
    }

    private void denseRegHisto(int[] regHisto) {
        for (int i = 0; i < registerSize(); i++) {
            regHisto[(int)denseGetRegister(i)]++;
        }
    }

    private boolean sparseRegHisto(int[] regHisto) {
        int p = headerLen();

        int idx = 0;
        while (p < buffer.length) {
            if (sparseIsZero(buffer[p])) {
                int runlen = sparseZeroLen(buffer[p]);
                idx += runlen;
                regHisto[0] += runlen;
                p++;
            } else if (sparseIsXZero(buffer[p])) {
                int runlen = sparseXZeroLen(buffer[p], buffer[p + 1]);
                idx += runlen;
                regHisto[0] += runlen;
                p += 2;
            } else {
                int runlen = sparseValLen(buffer[p]);
                int regVal = sparseValValue(buffer[p]);
                idx += runlen;
                regHisto[regVal] += runlen;
                p++;
            }
        }

        return idx == registerSize();
    }

    CountResult hllCount() {
        double m = registerSize();

        int[] regHisto = new int[64];

        byte encoding = buffer[magic().length];
        if (encoding == HllEncoding.DENSE.value) {
            denseRegHisto(regHisto);
        } else if (encoding == HllEncoding.SPARSE.value) {
            if (!sparseRegHisto(regHisto)) {
                return new CountResult(0, false);
            }
        } else {
            throw new IllegalStateException("invalid encoding : " + encoding);
        }

        double z = m * tau((m - regHisto[HLL_Q + 1]) / m);
        for (int i = HLL_Q; i >= 1; --i) {
            z += regHisto[i];
            z *= 0.5;
        }

        z += m * sigma(regHisto[0] / m);

        double E = Math.round(HLL_ALPHA_INF * m * m / z);
        return new CountResult((long)E, true);
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
}
