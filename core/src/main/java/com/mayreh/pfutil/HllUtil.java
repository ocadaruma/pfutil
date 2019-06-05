package com.mayreh.pfutil;

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

    /**
     * 64 bit version of MurmurHash2
     */
    public static long murmurHash64A(byte[] data, int seed) {
        int len = data.length;

        long m = 0xc6a4a7935bd1e995L;
        int r = 47;

        long h = (seed & 0xffffffffL) ^ (len * m);

        int p = 0;
        int end = len - (len & 7);

        while (p != end) {
            long k = (long)data[p] & 0xffL;
            k |= ((long)data[p + 1] & 0xffL) << 8;
            k |= ((long)data[p + 2] & 0xffL) << 16;
            k |= ((long)data[p + 3] & 0xffL) << 24;
            k |= ((long)data[p + 4] & 0xffL) << 32;
            k |= ((long)data[p + 5] & 0xffL) << 40;
            k |= ((long)data[p + 6] & 0xffL) << 48;
            k |= ((long)data[p + 7] & 0xffL) << 56;

            k *= m;
            k ^= k >>> r;
            k *= m;
            h ^= k;
            h *= m;

            p += 8;
        }

        switch (len & 7) {
            case 7: h ^= ((long)data[p + 6] & 0xffL) << 48;
            case 6: h ^= ((long)data[p + 5] & 0xffL) << 40;
            case 5: h ^= ((long)data[p + 4] & 0xffL) << 32;
            case 4: h ^= ((long)data[p + 3] & 0xffL) << 24;
            case 3: h ^= ((long)data[p + 2] & 0xffL) << 16;
            case 2: h ^= ((long)data[p + 1] & 0xffL) << 8;
            case 1:
                h ^= (long)data[p] & 0xffL;
                h *= m;
        }

        h ^= h >>> r;
        h *= m;
        h ^= h >>> r;

        return h;
    }
}
