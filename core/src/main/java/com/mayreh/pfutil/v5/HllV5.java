package com.mayreh.pfutil.v5;

import com.mayreh.pfutil.HllByteBuffer;

/**
 * Provides Redis v5 compatible HLL features.
 * <p>
 * NOTE: This class is NOT thread safe as in original C-implementation.
 * </p>
 */
public class HllV5 {
    private final HllhdrV5 hllhdr;

    HllV5(byte[] representation) {
        hllhdr = new HllhdrV5(representation);

        if (!hllhdr.isValidHll()) {
            throw new IllegalArgumentException("Invalid HLL representation");
        }
    }

    HllV5() {
        hllhdr = new HllhdrV5();
    }

    /**
     * Do PFCOUNT using same algorithm as of Redis v5.
     *
     * @return approximate distinct count
     */
    public long pfCount() {
        if (hllhdr.isValidCache()) {
            return hllhdr.getCache();
        } else {
            HllhdrV5.CountResult result = hllhdr.hllCount();
            if (!result.valid) {
                throw new IllegalStateException("hllCount result is invalid");
            }

            hllhdr.setCache(result.count);
            return result.count;
        }
    }

    /**
     * Do PFADD using mostly same algorithm as of Redis v5.
     * <p>
     * See {@link HllByteBuffer#hllSet(int, int)} for the differences.
     * </p>
     *
     * @param element the element to be added to HLL
     * @return whether HLL internal register was updated or not
     */
    public boolean pfAdd(byte[] element) {
        return hllhdr.hllAdd(element);
    }

    /**
     * Do PFMERGE using mostly same algorithm as of Redis v5.
     * <p>
     * See {@link HllByteBuffer#hllMerge(HllByteBuffer...)} for the differences.
     * </p>
     *
     * @param others HLLs to be merged
     * @return this HLL
     */
    public HllV5 pfMerge(HllV5... others) {
        if (others.length < 1) {
            return this;
        }
        HllhdrV5[] otherHlls = new HllhdrV5[others.length];
        for (int i = 0; i < others.length; i++) {
            otherHlls[i] = others[i].hllhdr;
        }

        hllhdr.hllMerge(otherHlls);
        hllhdr.invalidateCache();

        return this;
    }

    /**
     * Dump HLL representation as byte array.
     * <p>
     * The byte array will be same as one that can be retrieved as follows:
     * </p>
     * <pre>
     * {@code
     * redis-cli> PFADD key elem1, elem2, ....
     * redis-cli> GET key
     * }
     * </pre>
     *
     * @return the byte array of the HLL representation
     */
    public byte[] dumpRepr() {
        return hllhdr.dump();
    }

    public static class HllV5Builder {
        private byte[] representation = null;

        private HllV5Builder() {
        }

        /**
         * Restore HHL data structure from representation byte array
         *
         * @param representation Redis v5 HHL representation
         * @return builder instance
         */
        public HllV5Builder withRepr(byte[] representation) {
            this.representation = representation;
            return this;
        }

        public HllV5 build() {
            if (representation == null) {
                return new HllV5();
            } else {
                return new HllV5(representation);
            }
        }
    }

    public static HllV5Builder newBuilder() {
        return new HllV5Builder();
    }
}
