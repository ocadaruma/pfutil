package com.mayreh.pfutil.v4;

import com.mayreh.pfutil.RedisUtil;
import io.lettuce.core.api.sync.RedisCommands;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

public class HllV4Integration {
    private boolean shouldSkip() {
        return System.getenv("TEST_IT") == null;
    }

    @Test
    public void testSmallCardinality() {
        if (shouldSkip()) {
            return;
        }

        System.out.println("start small cardinality integration...");
        RedisUtil.withRedis(commands -> {
            List<String> elements;
            byte[] key;

            key = "pfutil:it100".getBytes();
            HllV4 hll100 = HllV4.newBuilder().build();
            commands.del(key);
            elements = randomizedAdd(commands, key, hll100, 100);

            assertThat(hll100.pfCount())
                    .as(elements.isEmpty() ? "check count" : "check count for elements: " + elements)
                    .isEqualTo(commands.pfcount(key).longValue());

            key = "pfutil:it1000".getBytes();
            HllV4 hll1000 = HllV4.newBuilder().build();
            commands.del(key);
            elements = randomizedAdd(commands, key, hll1000, 1000);

            assertThat(hll1000.pfCount())
                    .as(elements.isEmpty() ? "check count" : "check count for elements: " + elements)
                    .isEqualTo(commands.pfcount(key).longValue());

            key = "pfutil:it10000".getBytes();
            HllV4 hll10000 = HllV4.newBuilder().build();
            commands.del(key);
            elements = randomizedAdd(commands, key, hll10000, 10000);

            assertThat(hll10000.pfCount())
                    .as(elements.isEmpty() ? "check count" : "check count for elements: " + elements)
                    .isEqualTo(commands.pfcount(key).longValue());

            commands.pfmerge(
                    "pfutil:it100".getBytes(),
                    "pfutil:it1000".getBytes(),
                    "pfutil:it10000".getBytes());

            hll100.pfMerge(hll1000, hll10000);

            assertThat(hll100.pfCount()).isEqualTo(commands.pfcount("pfutil:it100".getBytes()).longValue());
        });
    }

    @Test
    public void testLargeCardinality() {
        if (shouldSkip()) {
            return;
        }

        System.out.println("start large cardinality integration...");
        RedisUtil.withRedis(commands -> {
            byte[] key;

            key = "pfutil:it100000".getBytes();
            HllV4 hll100000 = HllV4.newBuilder().build();
            commands.del(key);
            randomizedAdd(commands, key, hll100000, 100000);

            assertThat(hll100000.pfCount()).isEqualTo(commands.pfcount(key).longValue());

            key = "pfutil:it1000000".getBytes();
            HllV4 hll1000000 = HllV4.newBuilder().build();
            commands.del(key);
            randomizedAdd(commands, key, hll1000000, 1000000);

            assertThat(hll1000000.pfCount()).isEqualTo(commands.pfcount(key).longValue());

            commands.pfmerge(
                    "pfutil:it100000".getBytes(),
                    "pfutil:it1000000".getBytes());

            hll100000.pfMerge(hll1000000);

            assertThat(hll100000.pfCount()).isEqualTo(commands.pfcount("pfutil:it100000".getBytes()).longValue());
        });
    }

    private List<String> randomizedAdd(
            RedisCommands<byte[], byte[]> commands,
            byte[] redisKeyBytes,
            HllV4 hll,
            int num) {
        Random random = new Random();

        // dump elements when fail for small cardinality
        boolean dumpEnabled = num <= 1_000;

        List<String> elements = new ArrayList<>();

        // flush to Redis every 10 elements
        List<String> buffer = new ArrayList<>(10);

        for (int i = 0; i < num; i++) {
            String element = String.valueOf(random.nextInt());
            buffer.add(element);
            if (dumpEnabled) {
                elements.add(element);
            }

            hll.pfAdd(element.getBytes());
            if (buffer.size() == 10) {
                byte[][] elementsAsBytes = new byte[10][];
                for (int j = 0; j < 10; j++) {
                    elementsAsBytes[j] = buffer.get(j).getBytes();
                }
                commands.pfadd(redisKeyBytes, elementsAsBytes);
                buffer.clear();
            }
        }

        return elements;
    }
}