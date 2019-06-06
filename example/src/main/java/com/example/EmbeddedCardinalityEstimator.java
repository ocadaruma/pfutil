package com.example;

import com.mayreh.pfutil.v4.HllV4;

public class EmbeddedCardinalityEstimator {
    private final HllV4 hll;

    public EmbeddedCardinalityEstimator() {
        this.hll = HllV4.newBuilder().build();
    }

    public void add(String... elements) {
        for (String element : elements) {
            hll.pfAdd(element.getBytes());
        }
    }

    public void merge(byte[] hllData) {
        hll.pfMerge(HllV4.newBuilder().withRepr(hllData).build());
    }

    public byte[] dump() {
        return hll.dumpRepr();
    }
}
