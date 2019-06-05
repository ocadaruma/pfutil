package com.mayreh.pfutil.v4;

import com.mayreh.pfutil.HllUtil;
import lombok.RequiredArgsConstructor;
import lombok.Value;

import java.nio.ByteBuffer;

@RequiredArgsConstructor
class Sparse {
    private final Hllhdr.Config config;
    private final ByteBuffer buffer;

    private static final int HLL_SPARSE_XZERO_BIT = 0x40;

    @Value
    public static class SparseSumResult {
        boolean valid;
        Hllhdr.SumResult sum;
    }

    private static boolean sparseIsZero(byte b) {
        return (((int)b & 0xff) & 0xc0) == 0;
    }

    private static boolean sparseIsXZero(byte b) {
        return (((int)b & 0xff) & 0xc0) == HLL_SPARSE_XZERO_BIT;
    }

    private static int sparseZeroLen(byte b) {
        return (((int)b & 0xff) & 0x3f) + 1;
    }

    private static int sparseXZeroLen(byte b, byte nextB) {
        return ((((int)b & 0xff) & 0x3f) << 8) | ((int)nextB & 0xff) + 1;
    }

    private static int sparseValValue(byte b) {
        return ((((int)b & 0xff) >>> 2) & 0x1f) + 1;
    }

    private static int sparseValLen(byte b) {
        return (((int)b & 0xff) & 0x3) + 1;
    }

    private static void sparseXZeroSet(ByteBuffer buffer, int len) {
        int _l = len - 1;

        buffer.put((byte)((_l >>> 8) | HLL_SPARSE_XZERO_BIT));
        buffer.put((byte)(_l & 0xff));
    }

    public SparseSumResult sparseSum() {
        buffer.position(Hllhdr.HEADER_BYTES_LEN);

        double E = 0.0;
        int ez = 0;
        int idx = 0;

        while (buffer.hasRemaining()) {
            byte b = buffer.get();

            if (sparseIsZero(b)) {
                int runlen = sparseZeroLen(b);
                idx += runlen;
                ez += runlen;
            } else if (sparseIsXZero(b)) {
                int runlen = sparseXZeroLen(b, buffer.get());
                idx += runlen;
                ez += runlen;
            } else {
                int runlen = sparseValLen(b);
                int regVal = sparseValValue(b);
                idx += runlen;
                E += HllUtil.pow2(-regVal) * runlen;
            }
        }

        if (idx != config.hllRegisters()) {
            return new SparseSumResult(false, null);
        }
        E += ez;
        return new SparseSumResult(true, new Hllhdr.SumResult(ez, E));
    }

    public int sparseAdd(byte[] element) {
        throw new UnsupportedOperationException("to be implemented");
    }

    /**
     * Initialize given buffer as sparse representation
     */
    static void initialize(Hllhdr.Config config, ByteBuffer buffer) {
        buffer.position(Hllhdr.HEADER_BYTES_LEN);

        int aux = config.hllRegisters();
        while (aux > 0) {
            int xzero = config.getHllSparseXZeroMaxLen();
            if (xzero > aux) {
                xzero = aux;
            }
            sparseXZeroSet(buffer, xzero);
            aux -= xzero;
        }

        buffer.rewind();
        buffer.put(new byte[]{'H', 'Y', 'L', 'L'});
        buffer.put(Hllhdr.Encoding.HLL_SPARSE.value);
    }
}
