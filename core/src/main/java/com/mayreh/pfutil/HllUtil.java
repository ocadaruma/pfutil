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
}
