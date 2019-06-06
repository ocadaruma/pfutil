package com.mayreh.pfutil.v4;

import com.mayreh.pfutil.TestUtil;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

public class HllV4Test {
    @Test
    public void testInstantiate() {
        HllV4 hll = HllV4.newBuilder().build();

        assertThat(hll.pfCount()).isEqualTo(0L);
    }

    @Test
    public void testPfAdd() {
        HllV4 hll = HllV4.newBuilder().build();

        for (int i = 1; i <= 1000; i++) {
            hll.pfAdd(String.valueOf(i).getBytes(StandardCharsets.UTF_8));
        }
        // from observation using real Redis, result count should be 1001
        assertThat(hll.pfCount()).isEqualTo(1001L);

        hll.pfAdd("1001".getBytes(StandardCharsets.UTF_8));

        // from observation using real Redis, the count should be updated by adding 1001
        assertThat(hll.pfCount()).isEqualTo(1002L);
    }

    @Test
    public void testPfMerge() throws Exception {
        HllV4 hll = HllV4.newBuilder().build();
        HllV4 other = HllV4.newBuilder()
                .withRepr(TestUtil.getResourceAsBytes("v4/dense_cached_55527.dat"))
                .build();

        assertThat(hll.pfMerge(other).pfCount()).isEqualTo(55527L);
    }

    @Test
    public void testDump() throws Exception {
        HllV4 hll = HllV4.newBuilder().build();

        byte[] otherBytes = TestUtil.getResourceAsBytes("v4/dense_cached_55527.dat");
        HllV4 other = HllV4.newBuilder().withRepr(otherBytes).build();

        hll.pfMerge(other).pfCount();

        assertThat(Arrays.equals(hll.dumpRepr(), otherBytes)).isTrue();
    }

    @Test
    public void testCountFromCardinCache() {
        HllV4 hll = HllV4.newBuilder().build();
        hll.pfAdd("a".getBytes());
        hll.pfAdd("b".getBytes());
        hll.pfAdd("c".getBytes());
        hll.pfAdd("d".getBytes());

        assertThat(hll.pfCount()).isEqualTo(4L);
        assertThat(hll.pfCount()).isEqualTo(4L);
    }
}
