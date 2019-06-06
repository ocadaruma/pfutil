package com.example;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.ByteArrayCodec;

public class LettuceRedisClient implements AutoCloseable {
    private final RedisClient client;
    private final RedisCommands<byte[], byte[]> commands;

    public LettuceRedisClient(String host, int port) {
        client = RedisClient
                .create(RedisURI.builder()
                        .withHost(host)
                        .withPort(port)
                        .build());

        commands = client.connect(ByteArrayCodec.INSTANCE).sync();
    }

    public void pfAdd(String key, String... elements) {
        byte[][] elementBytes = new byte[elements.length][];

        for (int i = 0; i < elements.length; i++) {
            elementBytes[i] = elements[i].getBytes();
        }
        commands.pfadd(key.getBytes(), elementBytes);
    }

    public long pfCount(String key) {
        Long count = commands.pfcount(key.getBytes());
        return count != null ? count : 0L;
    }


    public void set(String key, byte[] data) {
        commands.set(key.getBytes(), data);
    }

    public byte[] get(String key) {
        return commands.get(key.getBytes());
    }

    @Override
    public void close() throws Exception {
        client.shutdown();
    }
}
