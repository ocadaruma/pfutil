package com.mayreh.pfutil.v4;

import com.mayreh.pfutil.TestUtil;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

public class HllhdrV4Test {
    @Test
    public void testIsValidHllObject() throws Exception {
        // invalid header
        HllhdrV4 hllhdr = new HllhdrV4(new byte[]{});
        assertThat(hllhdr.isValidHll()).isFalse();

        // invalid dense size
        hllhdr = new HllhdrV4(new byte[]{
                'H','Y','L','L',0x0,0x0,0x0,0x0,(byte)0xe7,(byte)0xd8,0x0,0x0,0x0,0x0,0x0,0x0
        });
        assertThat(hllhdr.isValidHll()).isFalse();

        // valid dense size
        hllhdr = new HllhdrV4(TestUtil.getResourceAsBytes("v4/dense_cached_55527.dat"));
        assertThat(hllhdr.isValidHll()).isTrue();
    }

    @Test
    public void testDenseHllCount() throws Exception {
        HllhdrV4 hllhdr = new HllhdrV4(TestUtil.getResourceAsBytes("v4/dense_cached_55527.dat"));

        HllhdrV4.CountResult result = hllhdr.hllCount();

        assertThat(result.valid).isTrue();
        assertThat(result.count).isEqualTo(55527L);
    }

    @Test
    public void testSparseHllCount() throws Exception {
        HllhdrV4 hllhdr = new HllhdrV4(TestUtil.getResourceAsBytes("v4/sparse_cached_1002.dat"));

        HllhdrV4.CountResult result = hllhdr.hllCount();

        assertThat(result.valid).isTrue();
        assertThat(result.count).isEqualTo(1002L);
    }

    @Test
    public void testCreateEmptyHll() {
        HllhdrV4 hllhdr = new HllhdrV4();

        assertThat(hllhdr.isValidHll()).isTrue();

        HllhdrV4.CountResult result = hllhdr.hllCount();

        assertThat(result.valid).isTrue();
        assertThat(result.count).isEqualTo(0L);
    }

    @Test
    public void testDenseHllAdd() throws Exception {
        HllhdrV4 hllhdr = new HllhdrV4(TestUtil.getResourceAsBytes("v4/dense_cached_55527.dat"));

        String[] elements = new String[]{
                "test", "test2", "test3", "test4", "test5", "test6", "test7"
        };
        for (String element : elements) {
            boolean updated = hllhdr.hllAdd(element.getBytes(StandardCharsets.UTF_8));

            // from observation using real Redis v4, HLL will not be updated until adding "test7"
            if (element.equals("test7")) {
                assertThat(updated).isTrue();
            } else {
                assertThat(updated).isFalse();
            }
        }

        HllhdrV4.CountResult countResult = hllhdr.hllCount();
        byte[] newRepr = hllhdr.dump();
        byte[] reprFromRedis = TestUtil.getResourceAsBytes("v4/dense_nocache_55531.dat");

        // header cache will be invalidated in outer layer
        // so skip equality check of header section here
        for (int i = 16; i < newRepr.length; i++) {
            assertThat(newRepr[i]).isEqualTo(reprFromRedis[i]);
        }
        assertThat(countResult.valid).isTrue();
        assertThat(countResult.count).isEqualTo(55531L);
    }

    @Test
    public void testHllMergeDense() throws Exception {
        byte[] otherBytes = TestUtil.getResourceAsBytes("v4/dense_cached_55531.dat");
        HllhdrV4 otherHll = new HllhdrV4(otherBytes);
        HllhdrV4 thisHll = new HllhdrV4();

        assertThat(thisHll.hllCount().count).isEqualTo(0L);

        thisHll.hllMerge(otherHll);

        assertThat(thisHll.hllCount().count).isEqualTo(55531L);

        byte[] newRepr = thisHll.dump();

        // header cache will be updated in outer layer
        // so skip equality check of header section here
        for (int i = 16; i < newRepr.length; i++) {
            assertThat(newRepr[i]).isEqualTo(otherBytes[i]);
        }
    }

    @Test
    public void testHllMergeSparse() throws Exception {
        byte[] otherBytes = TestUtil.getResourceAsBytes("v4/sparse_cached_1002.dat");
        HllhdrV4 otherHll = new HllhdrV4(otherBytes);
        HllhdrV4 thisHll = new HllhdrV4();

        assertThat(thisHll.hllCount().count).isEqualTo(0L);

        thisHll.hllMerge(otherHll);

        assertThat(thisHll.hllCount().count).isEqualTo(1002L);
    }

    @Test
    public void testHllMergeMultiple() throws Exception {
        byte[] denseBytes = TestUtil.getResourceAsBytes("v4/dense_cached_55531.dat");
        byte[] sparseBytes = TestUtil.getResourceAsBytes("v4/sparse_cached_AtoZ.dat");
        byte[] mergedBytesFromRedis = TestUtil.getResourceAsBytes("v4/dense_cached_55531+AtoZ_55581.dat");

        HllhdrV4 denseHll = new HllhdrV4(denseBytes);
        HllhdrV4 sparseHll = new HllhdrV4(sparseBytes);
        HllhdrV4 thisHll = new HllhdrV4();

        assertThat(thisHll.hllCount().count).isEqualTo(0L);

        thisHll.hllMerge(denseHll, sparseHll);

        assertThat(thisHll.hllCount().count).isEqualTo(55581L);

        byte[] newRepr = thisHll.dump();

        // header cache will be updated in outer layer
        // so skip equality check of header section here
        for (int i = 16; i < mergedBytesFromRedis.length; i++) {
            assertThat(newRepr[i]).isEqualTo(mergedBytesFromRedis[i]);
        }
    }
}
