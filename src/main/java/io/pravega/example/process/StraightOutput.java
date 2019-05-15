package io.pravega.example.process;

import io.pravega.example.data.random.RandomSensorSource;
import io.pravega.example.data.random.RandomStringSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

public class StraightOutput {
    public static void main(String[] args) throws Exception{
        // initialize the Flink execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        RandomSensorSource source = new RandomSensorSource(4, 10, 500);

        DataStream<Tuple2<Integer, Double>> dataStream = env
                .addSource(source);

        dataStream.keyBy(0)
                .timeWindow(Time.seconds(5));
        /* wordcount
                .map(new MyMapper())
                .keyBy(0)
                .sum(1);
        */

        // create an output sink to print to stdout for verification
        dataStream.print();

        // execute within the Flink environment
        env.execute("StraightOutput");
    }

    private static class MyMapper implements MapFunction<String, Tuple2<String, Integer>> {
        @Override
        public Tuple2<String, Integer> map(String s) {
            return Tuple2.of(s, 1);
        }
    }
}
