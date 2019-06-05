package com.mayreh.pfutil;

import org.junit.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

public class HllUtilTest {

    @Test
    public void testPow2() {
        assertThat(HllUtil.pow2(0)).isEqualTo(1.0);
        assertThat(HllUtil.pow2(8)).isEqualTo(256.0);
        assertThat(HllUtil.pow2(-8)).isEqualTo(0.00390625);
    }

    @Test
    public void testMurmurHash64A() {
        byte[] data = "Lorem ipsum dolor sit amet, consectetur adipisicing elit"
                .getBytes(StandardCharsets.UTF_8);

        long result = HllUtil.murmurHash64A(data, 0xe17a1465);
        assertThat(result).isEqualTo(0x0920e0c1b7eeb261L);
    }
}
