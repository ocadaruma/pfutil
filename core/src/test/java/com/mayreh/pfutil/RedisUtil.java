package com.mayreh.pfutil;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.ByteArrayCodec;

import java.util.function.Consumer;

public class RedisUtil {

    public static void withRedis(int port, Consumer<RedisCommands<byte[], byte[]>> f) {
        String host = System.getenv().getOrDefault("REDIS_HOST", "127.0.0.1");

        RedisClient client = null;
        try {
            client = RedisClient
                    .create(RedisURI.builder()
                            .withHost(host)
                            .withPort(port)
                            .build());
            f.accept(client.connect(ByteArrayCodec.INSTANCE).sync());
        } finally {
            if (client != null) {
                client.shutdown();
            }
        }
    }
}
