package com.mayreh.pfutil.benchmark;

import com.mayreh.pfutil.v4.HllV4;
import com.mayreh.pfutil.v5.HllV5;
import org.openjdk.jmh.annotations.*;

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static java.util.concurrent.TimeUnit.SECONDS;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(SECONDS)
@Threads(1)
@Fork(1)
@Warmup(iterations = 5, time = 1, timeUnit = SECONDS)
@Measurement(iterations = 10, time = 1, timeUnit = SECONDS)
public class HllBenchmark {
    private static final int SIZE = 1_000_000;

    @State(Scope.Thread)
    public static class PfAddState {
        Random r;
        byte[][] elements;
        HllV4 hllV4;
        HllV5 hllV5;

        @Setup(Level.Trial)
        public void setup() {
            r = ThreadLocalRandom.current();
        }

        @Setup(Level.Invocation)
        public void initialize() {
            hllV4 = HllV4.newBuilder().build();
            hllV5 = HllV5.newBuilder().build();
            elements = new byte[SIZE][];
            for (int i = 0; i < elements.length; i++) {
                elements[i] = UUID.randomUUID().toString().getBytes();
            }
        }
    }

    @State(Scope.Thread)
    public static class PfCountState {
        Random r;
        byte[][] elements;
        HllV4 hllV4;
        HllV5 hllV5;

        @Setup(Level.Trial)
        public void setup() {
            r = ThreadLocalRandom.current();
        }

        @Setup(Level.Invocation)
        public void initialize() {
            hllV4 = HllV4.newBuilder().build();
            hllV5 = HllV5.newBuilder().build();

            for (int i = 0; i < SIZE; i++) {
                hllV4.pfAdd(UUID.randomUUID().toString().getBytes());
                hllV5.pfAdd(UUID.randomUUID().toString().getBytes());
            }
        }
    }

    @State(Scope.Thread)
    public static class PfMergeState {
        Random r;

        HllV4 hllV4;
        HllV4 otherHllV4;

        HllV5 hllV5;
        HllV5 otherHllV5;

        @Setup(Level.Trial)
        public void setup() {
            r = ThreadLocalRandom.current();
        }

        @Setup(Level.Invocation)
        public void initialize() {
            hllV4 = HllV4.newBuilder().build();
            hllV5 = HllV5.newBuilder().build();

            otherHllV4 = HllV4.newBuilder().build();
            otherHllV5 = HllV5.newBuilder().build();

            for (int i = 0; i < SIZE; i++) {
                hllV4.pfAdd(UUID.randomUUID().toString().getBytes());
                hllV5.pfAdd(UUID.randomUUID().toString().getBytes());

                otherHllV4.pfAdd(UUID.randomUUID().toString().getBytes());
                otherHllV5.pfAdd(UUID.randomUUID().toString().getBytes());
            }
        }
    }

    @Benchmark
    @OperationsPerInvocation(SIZE)
    public void pfAddV4(PfAddState state) {
        for (byte[] element : state.elements) {
            state.hllV4.pfAdd(element);
        }
    }

    @Benchmark
    @OperationsPerInvocation(SIZE)
    public void pfAddV5(PfAddState state) {
        for (byte[] element : state.elements) {
            state.hllV5.pfAdd(element);
        }
    }

    @Benchmark
    public void pfCountV4(PfCountState state) {
        state.hllV4.pfCount();
    }

    @Benchmark
    public void pfCountV5(PfCountState state) {
        state.hllV5.pfCount();
    }

    @Benchmark
    public void pfMergeV4(PfMergeState state) {
        state.hllV4.pfMerge(state.otherHllV4);
    }

    @Benchmark
    public void pfMergeV5(PfMergeState state) {
        state.hllV5.pfMerge(state.otherHllV5);
    }
}
