package com.mayreh.pfutil;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.ByteArrayCodec;

import java.util.function.Consumer;

public class RedisUtil {

    public static void withRedis(Consumer<RedisCommands<byte[], byte[]>> f) {
        RedisClient client = null;
        try {
            client = RedisClient
                    .create(RedisURI.builder()
                            .withHost("127.0.0.1")
                            .withPort(6379)
                            .build());
            f.accept(client.connect(ByteArrayCodec.INSTANCE).sync());
        } finally {
            if (client != null) {
                client.shutdown();
            }
        }
    }
}
