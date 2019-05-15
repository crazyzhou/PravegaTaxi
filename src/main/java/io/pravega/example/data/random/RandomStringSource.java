package io.pravega.example.data.random;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import java.util.Random;

public class RandomStringSource implements SourceFunction<String> {
    // time(s) for source, -1 for infinity
    private final long duration;
    // time(ms) between two messages
    private final int servingSpeed;

    private static final String[] dict = {"apple", "banana", "cat", "dog", "elephant", "fish", "goat", "horse"};

    public RandomStringSource() {
        this(-1, 100);
    }

    public RandomStringSource(int servingSpeedFactor) {
        this(-1, servingSpeedFactor);
    }

    public RandomStringSource(long duration, int servingSpeedFactor) {
        this.duration = duration;
        this.servingSpeed = servingSpeedFactor;
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        long startTime = System.currentTimeMillis();
        Random random = new Random();

        while (System.currentTimeMillis() < startTime + duration * 1000) {
            Thread.sleep(servingSpeed);
            int index = random.nextInt(8);
            ctx.collectWithTimestamp(dict[index], System.currentTimeMillis());
        }
    }

    @Override
    public void cancel() {
    }
}
