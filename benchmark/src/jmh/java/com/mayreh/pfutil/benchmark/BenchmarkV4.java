package com.mayreh.pfutil.benchmark;

import com.mayreh.pfutil.v4.HllV4;
import org.openjdk.jmh.annotations.*;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import static java.util.concurrent.TimeUnit.SECONDS;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(SECONDS)
@Threads(1)
@Fork(1)
@Warmup(iterations = 5, time = 1, timeUnit = SECONDS)
@Measurement(iterations = 10, time = 1, timeUnit = SECONDS)
public class BenchmarkV4 {
    private static final int SIZE = 10_000;

    @State(Scope.Thread)
    public static class PfAddState {
        Random r;
        byte[][] elements;
        HllV4 hll;

        @Setup(Level.Trial)
        public void allocate() {
            r = ThreadLocalRandom.current();
            elements = new byte[SIZE][];
        }

        @Setup(Level.Invocation)
        public void initializeHll() {
            hll = HllV4.newBuilder().build();
            for (int i = 0; i < elements.length; i++) {
                elements[i] = String.valueOf(r.nextInt()).getBytes();
            }
        }

        @TearDown(Level.Trial)
        public void recycle() {
            hll = null;
            elements = null;
        }
    }

    @State(Scope.Thread)
    public static class PfMergeState {
        Random r;
        HllV4 hll;
        HllV4 otherHll;

        @Setup(Level.Trial)
        public void allocate() {
            r = ThreadLocalRandom.current();
        }

        @Setup(Level.Invocation)
        public void initializeHll() {
            hll = HllV4.newBuilder().build();
            otherHll = HllV4.newBuilder().build();

            for (int i = 0; i < 1_000_000; i++) {
                hll.pfAdd(String.valueOf(r.nextInt()).getBytes());
                otherHll.pfAdd(String.valueOf(r.nextInt()).getBytes());
            }
        }

        @TearDown(Level.Trial)
        public void recycle() {
            hll = null;
            otherHll = null;
        }
    }

    @Benchmark
    @OperationsPerInvocation(SIZE)
    public void benchmarkPfAdd(PfAddState state) {
        for (byte[] element : state.elements) {
            state.hll.pfAdd(element);
        }
    }

    @Benchmark
    public void benchmarkPfMerge(PfMergeState state) {
        state.hll.pfMerge(state.otherHll);
    }
}
