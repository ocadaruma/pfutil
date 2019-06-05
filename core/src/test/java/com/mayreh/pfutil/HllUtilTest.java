package com.mayreh.pfutil;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class HllUtilTest {

    @Test
    public void testPow2() {
        assertThat(HllUtil.pow2(0)).isEqualTo(1.0);
        assertThat(HllUtil.pow2(8)).isEqualTo(256.0);
        assertThat(HllUtil.pow2(-8)).isEqualTo(0.00390625);
    }
}
