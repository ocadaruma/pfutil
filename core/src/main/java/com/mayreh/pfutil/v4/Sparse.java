package com.mayreh.pfutil.v4;

import com.mayreh.pfutil.HllUtil;
import lombok.Value;

import java.nio.ByteBuffer;

class Sparse {
    private final Config config;
    private ByteBuffer buffer;

    public Sparse(Config config, ByteBuffer buffer) {
        this.config = config;
        this.buffer = buffer;
    }

    private static final int HLL_SPARSE_XZERO_BIT = 0x40;
    private static final int HLL_SPARSE_VAL_BIT = 0x80;

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

    private static boolean sparseIsVal(byte b) {
        return (((int)b & 0xff) & HLL_SPARSE_VAL_BIT) > 0;
    }

    private static int sparseZeroLen(byte b) {
        return (((int)b & 0xff) & 0x3f) + 1;
    }

    private static int sparseXZeroLen(byte b, byte nextB) {
        return (((((int)b & 0xff) & 0x3f) << 8) | ((int)nextB & 0xff)) + 1;
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

    private static void sparseZeroSet(ByteBuffer buffer, int len) {
        buffer.put((byte)(len - 1));
    }

    private static void sparseValSet(ByteBuffer buffer, int val, int len) {
        buffer.put((byte)(((val - 1) << 2) | (len - 1) | HLL_SPARSE_VAL_BIT));
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
        Hllhdr.PatLenResult result = Hllhdr.hllPatLen(config, element);

        return sparseSet(result.getReg(), result.getLen());
    }

    public int sparseSet(int index, int count) {
        if (count > config.hllSparseValMaxValue()) {
            return promote();
        }

        buffer.position(Hllhdr.HEADER_BYTES_LEN);

        int first = 0;
        int prevPos = -1;
        int nextPos = -1;
        int span = 0;
        while (buffer.hasRemaining()) {
            byte b = buffer.get();

            int oplen = 1;
            if (sparseIsZero(b)) {
                span = sparseZeroLen(b);
            } else if (sparseIsVal(b)) {
                span = sparseValLen(b);
            } else {
                span = sparseXZeroLen(b, buffer.get());
                oplen = 2;
            }
            if (index <= first + span - 1) {
                break;
            }
            prevPos = buffer.position() - 1;
            buffer.position(buffer.position() + oplen - 1);
            first += span;
        }

        if (span == 0) {
            return -1;
        }

        buffer.mark();
        if (buffer.position() < buffer.capacity() - 2) {
            if (sparseIsXZero(buffer.get())) {
                nextPos = buffer.position() + 1;
            }
            buffer.reset();
        } else {
            buffer.reset();
            if (buffer.position() < buffer.capacity() - 1) {
                nextPos = buffer.position() + 1;
            }
        }

        buffer.mark();
        byte b = buffer.get();
        boolean isZero = false, isXZero = false, isVal = false;
        int runlen;
        if (sparseIsZero(b)) {
            isZero = true;
            runlen = sparseZeroLen(b);
        } else if (sparseIsXZero(b)) {
            isXZero = true;
            runlen = sparseXZeroLen(b, buffer.get());
        } else {
            isVal = true;
            runlen = sparseValLen(b);
        }

        buffer.reset();
        int oldCount;
        if (isVal) {
            oldCount = sparseValValue(b);
            if (oldCount >= count) {
                return 0;
            }
            if (runlen == 1) {
                sparseValSet(buffer, count, 1);
                return updated(prevPos);
            }
        }

        if (isZero && runlen == 1) {
            sparseValSet(buffer, count, 1);
            return updated(prevPos);
        }

        throw new UnsupportedOperationException("");
    }

    private int updated(int prevPos) {
        int pos = prevPos > -1 ? prevPos : Hllhdr.HEADER_BYTES_LEN;

        buffer.position(pos);
        int scanlen = 5;
        while (buffer.hasRemaining() && scanlen-- > 0) {
            byte b = buffer.get();
            if (sparseIsXZero(b)) {
                // proceed
                buffer.get();
                continue;
            } else if (sparseIsZero(b)) {
                continue;
            }

            if (buffer.position() < buffer.capacity() - 1) {
                buffer.mark();
                byte b1 = buffer.get();
                buffer.reset();

                if (sparseIsVal(b1)) {
                    int v0 = sparseValValue(b);
                    int v1 = sparseValValue(b1);
                    if (v0 == v1) {

                    }
                }
            }
        }
        Hllhdr.invalidateCache(buffer);
        return 1;
    }

    private int promote() {
        throw new UnsupportedOperationException("");
    }

    /**
     * Initialize given buffer as sparse representation
     */
    static void initialize(Config config, ByteBuffer buffer) {
        buffer.position(Hllhdr.HEADER_BYTES_LEN);

        int aux = config.hllRegisters();
        while (aux > 0) {
            int xzero = config.hllSparseXZeroMaxLen();
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
