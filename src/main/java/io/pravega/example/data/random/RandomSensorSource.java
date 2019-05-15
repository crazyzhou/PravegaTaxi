package io.pravega.example.data.random;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class RandomSensorSource implements SourceFunction<Tuple2<Integer, Double>> {
    // number of sensors
    private final int sensorNum;
    // time(s) for source, -1 for infinity
    private final long duration;
    // time(ms) between two messages
    private final int servingSpeed;

    public RandomSensorSource() {
        this(4, -1, 100);
    }

    public RandomSensorSource(int servingSpeedFactor) {
        this(4, -1, servingSpeedFactor);
    }

    public RandomSensorSource(int sensorNum, long duration, int servingSpeedFactor) {
        this.sensorNum = sensorNum;
        this.duration = duration;
        this.servingSpeed = servingSpeedFactor;
    }

    @Override
    public void run(SourceContext<Tuple2<Integer, Double>> ctx) throws Exception {
        long startTime = System.currentTimeMillis();
        Random random = new Random();

        while (System.currentTimeMillis() < startTime + duration * 1000) {
            Thread.sleep(servingSpeed);
            int index = random.nextInt(sensorNum);
            Double value = random.nextDouble();
            ctx.collectWithTimestamp(Tuple2.of(index, value), System.currentTimeMillis());
        }
    }

    @Override
    public void cancel() {
    }
}
