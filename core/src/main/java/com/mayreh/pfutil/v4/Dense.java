package com.mayreh.pfutil.v4;

import com.mayreh.pfutil.HllUtil;
import lombok.RequiredArgsConstructor;

import java.nio.ByteBuffer;

@RequiredArgsConstructor
class Dense {
    private final Config config;
    private final ByteBuffer buffer;

    static long getRegisterAt(
            Config config,
            ByteBuffer buffer,
            int regnum) {
        int hllBits = config.hllBits();

        int byteOffset = Hllhdr.HEADER_BYTES_LEN + (regnum * hllBits / 8);
        long bitPosFromLSB = regnum * hllBits & 7;
        long bitPosFromLSBInNextByte = 8 - bitPosFromLSB;

        buffer.position(byteOffset);
        long b0 = (long)buffer.get() & 0xffL;
        long b1 = buffer.hasRemaining() ? (long)buffer.get() & 0xffL : 0;

        return ((b0 >>> bitPosFromLSB) | (b1 << bitPosFromLSBInNextByte)) & config.hllRegisterMax();
    }

    static void setRegisterAt(
            Config config,
            ByteBuffer buffer,
            int regnum,
            int count) {
        int hllBits = config.hllBits();

        int byteOffset = Hllhdr.HEADER_BYTES_LEN + (regnum * hllBits / 8);
        long bitPosFromLSB = regnum * hllBits & 7;
        long bitPosFromLSBInNextByte = 8 - bitPosFromLSB;

        long val = count & 0xffffffffL;

        buffer.position(byteOffset);
        buffer.mark();

        byte b0 = buffer.get();
        byte b1 = buffer.hasRemaining() ? buffer.get() : 0;

        b0 &= ~(config.hllRegisterMax() << bitPosFromLSB);
        b0 |= val << bitPosFromLSB;
        b1 &= ~(config.hllRegisterMax() >>> bitPosFromLSBInNextByte);
        b1 |= val >>> bitPosFromLSBInNextByte;

        buffer.reset();
        buffer.put(b0);

        if (buffer.hasRemaining()) {
            buffer.put(b1);
        }
    }

    public Hllhdr.SumResult denseSum() {
        int ez = 0;
        double E = 0;
        for (int i = 0; i < config.hllRegisters(); i++) {
            long reg = getRegisterAt(config, buffer, i);
            if (reg == 0) {
                ez++;
            } else {
                E += HllUtil.pow2(-(int)reg);
            }
        }
        E += ez;
        return new Hllhdr.SumResult(ez, E);
    }

    public int denseAdd(byte[] element) {
        Hllhdr.PatLenResult result = Hllhdr.hllPatLen(config, element);

        return denseSet(result.getReg(), result.getLen());
    }

    public int denseSet(int index, int count) {
        long old = getRegisterAt(config, buffer, index);

        if (count > old) {
            setRegisterAt(config, buffer, index, count);
            return 1;
        } else {
            return 0;
        }
    }
}
