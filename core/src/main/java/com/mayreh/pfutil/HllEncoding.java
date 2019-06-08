package com.mayreh.pfutil;

/**
 * Redis HLL encoding
 */
public enum HllEncoding {
    DENSE((byte)0),
    SPARSE((byte)(1));

    public final byte value;

    HllEncoding(byte value) {
        this.value = value;
    }
}
