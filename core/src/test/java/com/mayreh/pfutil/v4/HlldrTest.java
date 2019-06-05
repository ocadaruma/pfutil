package com.mayreh.pfutil.v4;

import com.mayreh.pfutil.TestUtil;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.assertThat;

public class HlldrTest {
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
        Hllhdr.Config config = Hllhdr.Config.DEFAULT;

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
        Hllhdr.Config config = Hllhdr.Config.DEFAULT;

        ByteBuffer buffer = ByteBuffer.wrap(TestUtil.getResourceAsBytes("v4/dense_cached_55527.dat"));
        Hllhdr hllhdr = new Hllhdr(config, buffer);

        Hllhdr.HllCountResult result = hllhdr.hllCount();

        assertThat(result.isValid()).isTrue();
        assertThat(result.getCount()).isEqualTo(55527L);
    }

    @Test
    public void testSparseHllCount() throws Exception {
        Hllhdr.Config config = Hllhdr.Config.DEFAULT;

        ByteBuffer buffer = ByteBuffer.wrap(TestUtil.getResourceAsBytes("v4/sparse_cached_1002.dat"));
        Hllhdr hllhdr = new Hllhdr(config, buffer);

        Hllhdr.HllCountResult result = hllhdr.hllCount();

        assertThat(result.isValid()).isTrue();
        assertThat(result.getCount()).isEqualTo(1002L);
    }
}
