package com.mayreh.pfutil;

import lombok.Value;

public class HllUtil {

    public static double pow2(int p) {
        if (p < 0) {
            return 1.0 / (1L << -p);
        }
        if (p > 0) {
            return 1L << p;
        }

        return 1.0;
    }

    @Value
    public static class PatLenResult {
        int len; // the length of the pattern 000..1 of the element hash
        long reg; // the register index the element hashes to
    }

    /**
     * 64 bit version of MurmurHash2
     */
    public static long murmurHash64A(String element, int seed) {
        byte[] elementBytes = element.getBytes();

        long m = 0xc6a4a7935bd1e995L;
        int r = 47;

        long h = seed ^ (element.length() * m);

        for (byte b : elementBytes) {
            
        }
    }

    public static PatLenResult hllPatLen(String element) {

    }
}
