package com.mayreh.pfutil.v4;

import java.nio.ByteBuffer;

/**
 * Provides Redis v4 compatible HLL features.
 *
 * NOTE: This class is NOT thread-safe since every operation possibly mutates underlying byte array as in original C-implementation.
 */
public class HllV4 {
    private final Hllhdr hllhdr;

    HllV4(Config config, byte[] representation) {
        ByteBuffer hdrBuffer = ByteBuffer.wrap(representation);
        hllhdr = new Hllhdr(config, hdrBuffer);

        if (!hllhdr.isValidHllObject()) {
            throw new IllegalArgumentException("Invalid HLL representation");
        }
    }

    HllV4(Config config) {
        hllhdr = new Hllhdr(config);
    }

    /**
     * Do PFCOUNT using same algorithm as of Redis v4.
     *
     * @return approximate distinct count
     */
    public long pfCount() {
        if (hllhdr.getHeader().isValidCache()) {
            return hllhdr.getHeader().getCardinality();
        } else {
            Hllhdr.HllCountResult result = hllhdr.hllCount();
            if (!result.isValid()) {
                throw new IllegalStateException("hllCount result is invalid");
            }

            hllhdr.setCache(result.getCount());
            return result.getCount();
        }
    }

    /**
     * Do PFADD using mostly same algorithm as of Redis v4.
     * @see Hllhdr#hllAdd(byte[]) for the diferrences.
     *
     * @param element the element to be added to HLL
     * @return whether HLL internal register was updated or not
     */
    public boolean pfAdd(byte[] element) {
        int retVal = hllhdr.hllAdd(element);
        if (retVal > 0) {
            hllhdr.invalidateCache();
            return true;
        } else {
            return false;
        }
    }

    /**
     * Do PFMERGE using mostly same algorithm as of Redis v4
     * @see Hllhdr#hllMerge(Hllhdr...) for the diferrences
     *
     * @param others HLLs to be merged
     * @return this HLL
     */
    public HllV4 pfMerge(HllV4... others) {
        if (others.length < 1) {
            return this;
        }
        Hllhdr[] otherHlls = new Hllhdr[others.length];
        for (int i = 0; i < others.length; i++) {
            otherHlls[i] = others[i].hllhdr;
        }

        hllhdr.hllMerge(otherHlls);
        hllhdr.invalidateCache();

        return this;
    }

    /**
     * Dump HLL representation as byte array.
     * The byte array will be same as one that can be retrieved as follows:
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

    static class HllV4Builder {
        private Config config = null;
        private byte[] representation = null;

        private HllV4Builder() {
        }

        ////////////////////////////////
        // Currently commented out because modifying default configuration is not fully tested
        ////////////////////////////////
        // public HllV4Builder withConfig(Config config) {
        //     this.config = config;
        //     return this;
        // }

        /**
         * Restore HHL data structure from representation byte array
         *
         * @param representation Redis v4 HHL representation
         * @return builder instance
         */
        public HllV4Builder withRepr(byte[] representation) {
            this.representation = representation;
            return this;
        }

        public HllV4 build() {
            if (config == null) {
                config = Config.DEFAULT;
            }
            if (representation == null) {
                return new HllV4(config);
            } else {
                return new HllV4(config, representation);
            }
        }
    }

    public static HllV4Builder newBuilder() {
        return new HllV4Builder();
    }
}
