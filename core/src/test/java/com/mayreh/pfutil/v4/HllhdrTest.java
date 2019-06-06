package com.mayreh.pfutil.v4;

import com.mayreh.pfutil.TestUtil;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

public class HllhdrTest {
    @Test
    public void testScanHeader() {
        // invalid data
        ByteBuffer buffer = ByteBuffer.wrap(new byte[]{});
        Hllhdr.Header header = Hllhdr.Header.scan(buffer);
        assertThat(header).isNull();

        // sparse cached
        buffer = ByteBuffer.wrap(new byte[]{
                'H','Y','L','L',0x1,0x0,0x0,0x0,0x1,0x0,0x0,0x0,0x0,0x0,0x0,0x0
        });
        header = Hllhdr.Header.scan(buffer);
        assertThat(header).isNotNull();
        assertThat(header.getEncoding()).isEqualTo(Hllhdr.Encoding.HLL_SPARSE);
        assertThat(header.isValidCache()).isTrue();
        assertThat(header.getCardinality()).isEqualTo(1L);
        assertThat(buffer.position()).isEqualTo(16);

        // sparse not cached
        buffer = ByteBuffer.wrap(new byte[]{
                'H','Y','L','L',0x1,0x0,0x0,0x0,0x0,0x0,0x0,0x0,0x0,0x0,0x0,(byte)0x80
        });
        header = Hllhdr.Header.scan(buffer);
        assertThat(header).isNotNull();
        assertThat(header.getEncoding()).isEqualTo(Hllhdr.Encoding.HLL_SPARSE);
        assertThat(header.isValidCache()).isFalse();
        assertThat(header.getCardinality()).isEqualTo(0L);
        assertThat(buffer.position()).isEqualTo(16);

        // dense cached
        buffer = ByteBuffer.wrap(new byte[]{
                'H','Y','L','L',0x0,0x0,0x0,0x0,(byte)0xe7,(byte)0xd8,0x0,0x0,0x0,0x0,0x0,0x0
        });
        header = Hllhdr.Header.scan(buffer);
        assertThat(header).isNotNull();
        assertThat(header.getEncoding()).isEqualTo(Hllhdr.Encoding.HLL_DENSE);
        assertThat(header.isValidCache()).isTrue();
        assertThat(header.getCardinality()).isEqualTo(55527L);
        assertThat(buffer.position()).isEqualTo(16);

        // dense not cached
        buffer = ByteBuffer.wrap(new byte[]{
                'H','Y','L','L',0x0,0x0,0x0,0x0,0x6d,0x4c,0x0,0x0,0x0,0x0,0x0,(byte)0x80
        });
        header = Hllhdr.Header.scan(buffer);
        assertThat(header).isNotNull();
        assertThat(header.getEncoding()).isEqualTo(Hllhdr.Encoding.HLL_DENSE);
        assertThat(header.isValidCache()).isFalse();
        assertThat(header.getCardinality()).isEqualTo(0L);
        assertThat(buffer.position()).isEqualTo(16);
    }

    @Test
    public void testIsValidHllObject() throws Exception {
        Config config = Config.DEFAULT;

        // invalid header
        ByteBuffer buffer = ByteBuffer.wrap(new byte[]{});
        Hllhdr hllhdr = new Hllhdr(config, buffer);
        assertThat(hllhdr.isValidHllObject()).isFalse();

        // invalid dense size
        buffer = ByteBuffer.wrap(new byte[]{
                'H','Y','L','L',0x0,0x0,0x0,0x0,(byte)0xe7,(byte)0xd8,0x0,0x0,0x0,0x0,0x0,0x0
        });
        hllhdr = new Hllhdr(config, buffer);
        assertThat(hllhdr.isValidHllObject()).isFalse();

        // valid dense size
        buffer = ByteBuffer.wrap(TestUtil.getResourceAsBytes("v4/dense_cached_55527.dat"));
        hllhdr = new Hllhdr(config, buffer);
        assertThat(hllhdr.isValidHllObject()).isTrue();
    }

    @Test
    public void testDenseHllCount() throws Exception {
        Config config = Config.DEFAULT;

        ByteBuffer buffer = ByteBuffer.wrap(TestUtil.getResourceAsBytes("v4/dense_cached_55527.dat"));
        Hllhdr hllhdr = new Hllhdr(config, buffer);

        Hllhdr.HllCountResult result = hllhdr.hllCount();

        assertThat(result.isValid()).isTrue();
        assertThat(result.getCount()).isEqualTo(55527L);
    }

    @Test
    public void testSparseHllCount() throws Exception {
        Config config = Config.DEFAULT;

        ByteBuffer buffer = ByteBuffer.wrap(TestUtil.getResourceAsBytes("v4/sparse_cached_1002.dat"));
        Hllhdr hllhdr = new Hllhdr(config, buffer);

        Hllhdr.HllCountResult result = hllhdr.hllCount();

        assertThat(result.isValid()).isTrue();
        assertThat(result.getCount()).isEqualTo(1002L);
    }

    @Test
    public void testCreateEmptyHll() {
        Config config = Config.DEFAULT;

        Hllhdr hllhdr = new Hllhdr(config);

        assertThat(hllhdr.getHeader()).isNotNull();
        assertThat(hllhdr.getHeader().getEncoding()).isEqualTo(Hllhdr.Encoding.HLL_SPARSE);

        Hllhdr.HllCountResult result = hllhdr.hllCount();

        assertThat(result.isValid()).isTrue();
        assertThat(result.getCount()).isEqualTo(0L);
    }

    @Test
    public void testDenseHllAdd() throws Exception {
        Config config = Config.DEFAULT;

        ByteBuffer buffer = ByteBuffer.wrap(TestUtil.getResourceAsBytes("v4/dense_cached_55527.dat"));
        Hllhdr hllhdr = new Hllhdr(config, buffer);

        String[] elements = new String[]{
                "test", "test2", "test3", "test4", "test5", "test6", "test7"
        };
        for (String element : elements) {
            int updated = hllhdr.hllAdd(element.getBytes(StandardCharsets.UTF_8));

            // from observation using real Redis v4, HLL will not be updated until adding "test7"
            if (element.equals("test7")) {
                assertThat(updated).isEqualTo(1);
            } else {
                assertThat(updated).isEqualTo(0);
            }
        }

        Hllhdr.HllCountResult countResult = hllhdr.hllCount();
        byte[] newRepr = hllhdr.dump();
        byte[] reprFromRedis = TestUtil.getResourceAsBytes("v4/dense_nocache_55531.dat");

        // header cache will be invalidated in outer layer
        // so skip equality check of header section here
        for (int i = 16; i < newRepr.length; i++) {
            assertThat(newRepr[i]).isEqualTo(reprFromRedis[i]);
        }
        assertThat(countResult.isValid()).isTrue();
        assertThat(countResult.getCount()).isEqualTo(55531L);
    }

    @Test
    public void testHllSparseToDense() throws Exception {
        Config config = Config.DEFAULT;

        ByteBuffer buffer = ByteBuffer.wrap(TestUtil.getResourceAsBytes("v4/sparse_cached_1002.dat"));
        Hllhdr hllhdr = new Hllhdr(config, buffer);

        assertThat(hllhdr.getHeader()).isNotNull();
        assertThat(hllhdr.getHeader().getEncoding()).isEqualTo(Hllhdr.Encoding.HLL_SPARSE);

        hllhdr.hllSparseToDense();
        assertThat(hllhdr.getHeader().getEncoding()).isEqualTo(Hllhdr.Encoding.HLL_DENSE);

        Hllhdr.HllCountResult countResult = hllhdr.hllCount();
        assertThat(countResult.isValid()).isTrue();
        assertThat(countResult.getCount()).isEqualTo(1002L);
    }

    @Test
    public void testHllMergeDense() throws Exception {
        Config config = Config.DEFAULT;

        byte[] otherBytes = TestUtil.getResourceAsBytes("v4/dense_cached_55531.dat");
        Hllhdr otherHll = new Hllhdr(config, ByteBuffer.wrap(otherBytes));
        Hllhdr thisHll = new Hllhdr(config);

        assertThat(thisHll.hllCount().getCount()).isEqualTo(0L);

        thisHll.hllMerge(otherHll);

        assertThat(thisHll.getHeader()).isNotNull();
        assertThat(thisHll.getHeader().getEncoding()).isEqualTo(Hllhdr.Encoding.HLL_DENSE);
        assertThat(thisHll.hllCount().getCount()).isEqualTo(55531L);

        byte[] newRepr = thisHll.dump();

        // header cache will be updated in outer layer
        // so skip equality check of header section here
        for (int i = 16; i < newRepr.length; i++) {
            assertThat(newRepr[i]).isEqualTo(otherBytes[i]);
        }
    }

    @Test
    public void testHllMergeSparse() throws Exception {
        Config config = Config.DEFAULT;

        byte[] otherBytes = TestUtil.getResourceAsBytes("v4/sparse_cached_1002.dat");
        Hllhdr otherHll = new Hllhdr(config, ByteBuffer.wrap(otherBytes));
        Hllhdr thisHll = new Hllhdr(config);

        assertThat(thisHll.hllCount().getCount()).isEqualTo(0L);

        thisHll.hllMerge(otherHll);

        assertThat(thisHll.getHeader()).isNotNull();
        assertThat(thisHll.getHeader().getEncoding()).isEqualTo(Hllhdr.Encoding.HLL_DENSE);
        assertThat(thisHll.hllCount().getCount()).isEqualTo(1002L);
    }

    @Test
    public void testHllMergeMultiple() throws Exception {
        Config config = Config.DEFAULT;

        byte[] denseBytes = TestUtil.getResourceAsBytes("v4/dense_cached_55531.dat");
        byte[] sparseBytes = TestUtil.getResourceAsBytes("v4/sparse_cached_AtoZ.dat");
        byte[] mergedBytesFromRedis = TestUtil.getResourceAsBytes("v4/dense_cached_55531+AtoZ_55581.dat");

        Hllhdr denseHll = new Hllhdr(config, ByteBuffer.wrap(denseBytes));
        Hllhdr sparseHll = new Hllhdr(config, ByteBuffer.wrap(sparseBytes));
        Hllhdr thisHll = new Hllhdr(config);

        assertThat(thisHll.hllCount().getCount()).isEqualTo(0L);

        thisHll.hllMerge(denseHll, sparseHll);

        assertThat(thisHll.getHeader()).isNotNull();
        assertThat(thisHll.getHeader().getEncoding()).isEqualTo(Hllhdr.Encoding.HLL_DENSE);
        assertThat(thisHll.hllCount().getCount()).isEqualTo(55581L);

        byte[] newRepr = thisHll.dump();

        // header cache will be updated in outer layer
        // so skip equality check of header section here
        for (int i = 16; i < mergedBytesFromRedis.length; i++) {
            assertThat(newRepr[i]).isEqualTo(mergedBytesFromRedis[i]);
        }
    }
}