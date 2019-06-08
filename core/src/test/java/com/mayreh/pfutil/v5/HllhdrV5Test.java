package com.mayreh.pfutil.v5;

import com.mayreh.pfutil.TestUtil;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

public class HllhdrV5Test {
    @Test
    public void testIsValidHllObject() throws Exception {
        // invalid header
        HllhdrV5 hllhdr = new HllhdrV5(new byte[]{});
        assertThat(hllhdr.isValidHll()).isFalse();

        // invalid dense size
        hllhdr = new HllhdrV5(new byte[]{
                'H','Y','L','L',0x0,0x0,0x0,0x0,(byte)0xe7,(byte)0xd8,0x0,0x0,0x0,0x0,0x0,0x0
        });
        assertThat(hllhdr.isValidHll()).isFalse();

        // valid dense size
        hllhdr = new HllhdrV5(TestUtil.getResourceAsBytes("v5/dense_cached_99562.dat"));
        assertThat(hllhdr.isValidHll()).isTrue();
    }

    @Test
    public void testDenseHllCount() throws Exception {
        HllhdrV5 hllhdr = new HllhdrV5(TestUtil.getResourceAsBytes("v5/dense_cached_99562.dat"));

        HllhdrV5.CountResult result = hllhdr.hllCount();

        assertThat(result.valid).isTrue();
        assertThat(result.count).isEqualTo(99562L);
    }

    @Test
    public void testSparseHllCount() throws Exception {
        HllhdrV5 hllhdr = new HllhdrV5(TestUtil.getResourceAsBytes("v5/sparse_cached_1001.dat"));

        HllhdrV5.CountResult result = hllhdr.hllCount();

        assertThat(result.valid).isTrue();
        assertThat(result.count).isEqualTo(1001L);
    }

    @Test
    public void testCreateEmptyHll() {
        HllhdrV5 hllhdr = new HllhdrV5();

        assertThat(hllhdr.isValidHll()).isTrue();

        HllhdrV5.CountResult result = hllhdr.hllCount();

        assertThat(result.valid).isTrue();
        assertThat(result.count).isEqualTo(0L);
    }

    @Test
    public void testDenseHllAdd() throws Exception {
        HllhdrV5 hllhdr = new HllhdrV5(TestUtil.getResourceAsBytes("v5/dense_cached_99562.dat"));

        String[] elements = new String[]{
                "test", "test2", "test3", "test4", "test5", "test6", "test7"
        };
        for (String element : elements) {
            boolean updated = hllhdr.hllAdd(element.getBytes(StandardCharsets.UTF_8));

            // from observation using real Redis v5, HLL will not be updated until adding "test7"
            if (element.equals("test7")) {
                assertThat(updated).isTrue();
            } else {
                assertThat(updated).isFalse();
            }
        }

        HllhdrV5.CountResult countResult = hllhdr.hllCount();
        byte[] newRepr = hllhdr.dump();
        byte[] reprFromRedis = TestUtil.getResourceAsBytes("v5/dense_cached_99571.dat");

        // header cache will be invalidated in outer layer
        // so skip equality check of header section here
        for (int i = 16; i < newRepr.length; i++) {
            assertThat(newRepr[i]).isEqualTo(reprFromRedis[i]);
        }
        assertThat(countResult.valid).isTrue();
        assertThat(countResult.count).isEqualTo(99571L);
    }

    @Test
    public void testHllMergeDense() throws Exception {
        byte[] otherBytes = TestUtil.getResourceAsBytes("v5/dense_cached_99562.dat");
        HllhdrV5 otherHll = new HllhdrV5(otherBytes);
        HllhdrV5 thisHll = new HllhdrV5();

        assertThat(thisHll.hllCount().count).isEqualTo(0L);

        thisHll.hllMerge(otherHll);

        assertThat(thisHll.hllCount().count).isEqualTo(99562L);

        byte[] newRepr = thisHll.dump();

        // header cache will be updated in outer layer
        // so skip equality check of header section here
        for (int i = 16; i < newRepr.length; i++) {
            assertThat(newRepr[i]).isEqualTo(otherBytes[i]);
        }
    }

    @Test
    public void testHllMergeSparse() throws Exception {
        byte[] otherBytes = TestUtil.getResourceAsBytes("v5/sparse_cached_1001.dat");
        HllhdrV5 otherHll = new HllhdrV5(otherBytes);
        HllhdrV5 thisHll = new HllhdrV5();

        assertThat(thisHll.hllCount().count).isEqualTo(0L);

        thisHll.hllMerge(otherHll);

        assertThat(thisHll.hllCount().count).isEqualTo(1001L);
    }

    @Test
    public void testHllMergeMultiple() throws Exception {
        byte[] denseBytes = TestUtil.getResourceAsBytes("v5/dense_cached_99571.dat");
        byte[] sparseBytes = TestUtil.getResourceAsBytes("v5/sparse_cached_AtoZ.dat");
        byte[] mergedBytesFromRedis = TestUtil.getResourceAsBytes("v5/dense_cached_99571+AtoZ_99607.dat");

        HllhdrV5 denseHll = new HllhdrV5(denseBytes);
        HllhdrV5 sparseHll = new HllhdrV5(sparseBytes);
        HllhdrV5 thisHll = new HllhdrV5();

        assertThat(thisHll.hllCount().count).isEqualTo(0L);

        thisHll.hllMerge(denseHll, sparseHll);

        assertThat(thisHll.hllCount().count).isEqualTo(99607L);

        byte[] newRepr = thisHll.dump();

        // header cache will be updated in outer layer
        // so skip equality check of header section here
        for (int i = 16; i < mergedBytesFromRedis.length; i++) {
            assertThat(newRepr[i]).isEqualTo(mergedBytesFromRedis[i]);
        }
    }
}
