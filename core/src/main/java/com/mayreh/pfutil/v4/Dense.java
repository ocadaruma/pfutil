package com.mayreh.pfutil.v4;

import com.mayreh.pfutil.HllUtil;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
class Dense {
    private final Hllhdr.Config config;
    private final byte[] registers;

    private static long getRegisterAt(
            Hllhdr.Config config,
            byte[] registers,
            int regnum) {
        int hllBits = config.getHllBits();

        int byteOffset = regnum * hllBits / 8;
        long bitPosFromLSB = regnum * hllBits & 7;
        long bitPosFromLSBInNextByte = 8 - bitPosFromLSB;

        long b0 = (long)registers[byteOffset] & 0xFFL;
        long b1 = byteOffset < registers.length - 1 ? (long)registers[byteOffset + 1] & 0xFFL : 0;

        return ((b0 >> bitPosFromLSB) | (b1 << bitPosFromLSBInNextByte)) & config.hllRegisterMax();
    }

    public Hllhdr.SumResult denseSum() {
        int ez = 0;
        double E = 0;
        for (int i = 0; i < config.hllRegisters(); i++) {
            long reg = getRegisterAt(config, registers, i);
            if (reg == 0) {
                ez++;
            } else {
                E += HllUtil.pow2(-(int)reg);
            }
        }
        E += ez;
        return new Hllhdr.SumResult(ez, E);
    }
}
