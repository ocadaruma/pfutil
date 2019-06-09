package com.mayreh.pfutil;

import java.util.Arrays;

/**
 * HLL algorithm-independent data structure which is compatible with Redis
 */
public abstract class HllByteBuffer {
    private static final int HEADER_LEN = 16;

    private static final int HLL_P = 14;
    private static final int HLL_REGISTERS = 1 << HLL_P;
    private static final int HLL_BITS = 6;
    private static final int HLL_REGISTER_MAX = (1 << HLL_BITS) - 1;

    private static final int HLL_DENSE_SIZE = HEADER_LEN + ((HLL_REGISTERS * HLL_BITS + 7) / 8);
    private static final int HLL_SPARSE_XZERO_MAX_LEN = 16384;
    private static final int HLL_SPARSE_XZERO_BIT = 0x40;

    private static final byte[] magic = new byte[]{'H', 'Y', 'L', 'L'};

    protected byte[] buffer;

    /**
     * Allocate new sparse byte buffer.
     */
    public HllByteBuffer() {
        int sparseLen =
                HEADER_LEN + (((HLL_REGISTERS + (HLL_SPARSE_XZERO_MAX_LEN - 1)) / HLL_SPARSE_XZERO_MAX_LEN) * 2);

        this.buffer = new byte[sparseLen];

        int p = HEADER_LEN;
        int aux = HLL_REGISTERS;
        while (aux > 0) {
            int xzero = HLL_SPARSE_XZERO_MAX_LEN;
            if (xzero > aux) {
                xzero = aux;
            }
            sparseXZeroSet(p, xzero);
            p += 2;
            aux -= xzero;
        }

        System.arraycopy(magic, 0, buffer, 0, magic.length);
        buffer[magic.length] = HllEncoding.SPARSE.value;
    }

    /**
     * Instantiate byte buffer from existing representation.
     *
     * @param repr existing HLL representation
     */
    public HllByteBuffer(byte[] repr) {
        this.buffer = Arrays.copyOf(repr, repr.length);
    }

    protected static int registerSize() {
        return HLL_REGISTERS;
    }

    protected static int headerLen() {
        return HEADER_LEN;
    }

    protected static int registerBits() {
        return HLL_P;
    }

    protected static int registerBitsMask() {
        return HLL_REGISTERS - 1;
    }

    protected static byte[] magic() {
        return magic;
    }

    /**
     * Validate if the underlying bytes is a valid HLL representation.
     *
     * @return validity
     */
    public boolean isValidHll() {
        if (buffer.length < HEADER_LEN) {
            return false;
        }

        int p = 0;
        while (p < magic.length) {
            if (buffer[p] != magic[p]) {
                return false;
            }
            p++;
        }

        HllEncoding encoding;
        switch (buffer[p]) {
            case 0:
                encoding = HllEncoding.DENSE;
                break;
            case 1:
                encoding = HllEncoding.SPARSE;
                break;
            default:
                return false;
        }

        if (encoding == HllEncoding.DENSE &&
                buffer.length != HLL_DENSE_SIZE) {
            return false;
        }

        return true;
    }

    /**
     * Invalidate cardinality cache.
     */
    public void invalidateCache() {
        // set position to cache flag
        int p = 15;
        buffer[p] = (byte)(buffer[p] | (1 << 7));
    }

    /**
     * Set cardinality cache to given value.
     *
     * @param count New cardinality to be set
     */
    public void setCache(long count) {
        // set position to the beginning of Cardin.
        int p = 8;

        buffer[p] = (byte)(count & 0xff);
        buffer[p + 1] = (byte)((count >>> 8) & 0xff);
        buffer[p + 2] = (byte)((count >>> 16) & 0xff);
        buffer[p + 3] = (byte)((count >>> 24) & 0xff);
        buffer[p + 4] = (byte)((count >>> 32) & 0xff);
        buffer[p + 5] = (byte)((count >>> 40) & 0xff);
        buffer[p + 6] = (byte)((count >>> 48) & 0xff);
        buffer[p + 7] = (byte)((count >>> 56) & 0xff);
    }

    /**
     * Get cardinality from cache in header.
     *
     * @return Cached cardinality
     */
    public long getCache() {
        // set position to the beginning of Cardin.
        int p = 8;

        long cardinality;
        cardinality = (long)buffer[p] & 0xffL;
        cardinality |= ((long)buffer[p + 1] & 0xffL) << 8;
        cardinality |= ((long)buffer[p + 2] & 0xffL) << 16;
        cardinality |= ((long)buffer[p + 3] & 0xffL) << 24;
        cardinality |= ((long)buffer[p + 4] & 0xffL) << 32;
        cardinality |= ((long)buffer[p + 5] & 0xffL) << 40;
        cardinality |= ((long)buffer[p + 6] & 0xffL) << 48;
        cardinality |= ((long)buffer[p + 7] & 0xffL) << 56;

        return cardinality;
    }

    /**
     * Return if the cardinality is valid.
     *
     * @return The validity of the cache
     */
    public boolean isValidCache() {
        return (buffer[15] & (1<<7)) == 0;
    }

    /**
     * Set specified register to given value.
     * <p>
     * NOTE: Unlike original Redis implementation, the representation always be promoted to
     * dense representation regardless of current encoding for simplification.
     * </p>
     *
     * @param regNum register number
     * @param len patLen count
     * @return 1 if the cardinality changed, 0 if not changed, -1 if an error is occurred
     */
    public int hllSet(int regNum, int len) {
        switch (buffer[magic.length]) {
            case 0:
                return denseSetIfNeeded(regNum, len);
            case 1:
                promoteSparseToDense();
                return denseSetIfNeeded(regNum, len);
            default:
                return -1;
        }
    }

    /**
     * Dump current HLL representation.
     *
     * @return The snapshot of current HLL
     */
    public byte[] dump() {
        return Arrays.copyOf(buffer, buffer.length);
    }

    // sparse operations

    protected boolean sparseIsZero(byte b) {
        return (((int)b & 0xff) & 0xc0) == 0;
    }

    protected boolean sparseIsXZero(byte b) {
        return (((int)b & 0xff) & 0xc0) == HLL_SPARSE_XZERO_BIT;
    }

    protected int sparseZeroLen(byte b) {
        return (((int)b & 0xff) & 0x3f) + 1;
    }

    protected int sparseXZeroLen(byte b, byte nextB) {
        return (((((int)b & 0xff) & 0x3f) << 8) | ((int)nextB & 0xff)) + 1;
    }

    protected int sparseValValue(byte b) {
        return ((((int)b & 0xff) >>> 2) & 0x1f) + 1;
    }

    protected int sparseValLen(byte b) {
        return (((int)b & 0xff) & 0x3) + 1;
    }

    private void sparseXZeroSet(int p, int len) {
        int _l = len - 1;

        buffer[p] = (byte)((_l >>> 8) | HLL_SPARSE_XZERO_BIT);
        buffer[p + 1] = (byte)(_l & 0xff);
    }

    // dense oprations

    protected long denseGetRegister(int regNum) {
        int byteOffset = HEADER_LEN + (regNum * HLL_BITS / 8);
        long bitPosFromLSB = regNum * HLL_BITS & 7;
        long bitPosFromLSBInNextByte = 8 - bitPosFromLSB;

        long b0 = (long)buffer[byteOffset] & 0xffL;
        long b1 = (byteOffset < buffer.length - 1) ? (long)buffer[byteOffset + 1] & 0xffL : 0;

        return ((b0 >>> bitPosFromLSB) | (b1 << bitPosFromLSBInNextByte)) & HLL_REGISTER_MAX;
    }

    private void denseSetRegister(int regNum, int count) {
        int byteOffset = HEADER_LEN + (regNum * HLL_BITS / 8);
        long bitPosFromLSB = regNum * HLL_BITS & 7;
        long bitPosFromLSBInNextByte = 8 - bitPosFromLSB;

        long val = count & 0xffffffffL;

        byte b0 = buffer[byteOffset];
        byte b1 = (byteOffset < buffer.length - 1) ? buffer[byteOffset + 1] : 0;

        b0 &= ~(HLL_REGISTER_MAX << bitPosFromLSB);
        b0 |= val << bitPosFromLSB;
        b1 &= ~(HLL_REGISTER_MAX >>> bitPosFromLSBInNextByte);
        b1 |= val >>> bitPosFromLSBInNextByte;

        buffer[byteOffset] = b0;

        if (byteOffset < buffer.length - 1) {
            buffer[byteOffset + 1] = b1;
        }
    }

    private int denseSetIfNeeded(int regNum, int count) {
        if (denseGetRegister(regNum) < count) {
            denseSetRegister(regNum, count);
            return 1;
        } else {
            return 0;
        }
    }

    /**
     * Promote sparse representation to dense.
     * <p>
     * Underlying buffer will be replaced to newly allocated dense buffer.
     * </p>
     */
    private void promoteSparseToDense() {
        // nothing to do
        if (buffer[magic.length] == HllEncoding.DENSE.value) {
            return;
        }

        byte[] denseBuffer = new byte[HLL_DENSE_SIZE];

        // copy header
        System.arraycopy(buffer, 0, denseBuffer, 0, HEADER_LEN);

        // skip magic and put encoding
        denseBuffer[magic.length] = HllEncoding.DENSE.value;

        int p = HEADER_LEN;
        int idx = 0;
        while (p < buffer.length) {
            if (sparseIsZero(buffer[p])) {
                int runlen = sparseZeroLen(buffer[p]);
                idx += runlen;
                p++;
            } else if (sparseIsXZero(buffer[p])) {
                int runlen = sparseXZeroLen(buffer[p], buffer[p + 1]);
                idx += runlen;
                p += 2;
            } else {
                int runlen = sparseValLen(buffer[p]);
                int regVal = sparseValValue(buffer[p]);

                while(runlen-- > 0) {
                    denseSetRegister(idx, regVal);
                    idx++;
                }
                p++;
            }
        }

        if (idx != HLL_REGISTERS) {
            throw new RuntimeException("failed to promote to dense");
        }

        this.buffer = denseBuffer;
    }

    /**
     * Merge given HLLs into this HLL
     * <p>
     * NOTE: Unlike original Redis implementation, the representation always be promoted to
     * dense representation regardless of current encoding for simplification.
     * </p>
     */
    public void hllMerge(HllByteBuffer... others) {
        byte[] max = new byte[HLL_REGISTERS];
        hllMergeRegisters(max, this);
        hllMergeRegisters(max, others);

        if (buffer[magic.length] == HllEncoding.SPARSE.value) {
            promoteSparseToDense();
        }

        for (int i = 0; i < max.length; i++) {
            denseSetRegister(i, max[i]);
        }
    }

    /**
     * Merge registers of given HLLs then write to target array
     */
    private static void hllMergeRegisters(byte[] max, HllByteBuffer... hlls) {
        for (HllByteBuffer hll : hlls) {
            boolean isDense = hll.buffer[magic.length] == HllEncoding.DENSE.value;
            if (isDense) {
                for (int i = 0; i < max.length; i++) {
                    long val = hll.denseGetRegister(i);
                    if (val > (max[i] & 0xffL)) {
                        max[i] = (byte)val;
                    }
                }
            } else {
                int p = HEADER_LEN;
                int idx = 0;
                while (p < hll.buffer.length) {
                    long runlen;
                    if (hll.sparseIsZero(hll.buffer[p])) {
                        runlen = hll.sparseZeroLen(hll.buffer[p]);
                        idx += runlen;
                        p++;
                    } else if (hll.sparseIsXZero(hll.buffer[p])) {
                        runlen = hll.sparseXZeroLen(hll.buffer[p], hll.buffer[p + 1]);
                        idx += runlen;
                        p += 2;
                    } else {
                        runlen = hll.sparseValLen(hll.buffer[p]);
                        int regVal = hll.sparseValValue(hll.buffer[p]);
                        while (runlen-- > 0) {
                            if (regVal > (max[idx] & 0xff)) {
                                max[idx] = (byte)regVal;
                            }
                            idx++;
                        }
                        p++;
                    }
                }
                if (idx != HLL_REGISTERS) {
                    throw new RuntimeException("failed to merge");
                }
            }
        }
    }
}
