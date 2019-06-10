package io.pravega.example.process;

import io.pravega.example.data.random.RandomSensorSource;
import io.pravega.example.data.random.RandomStringSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;

public class StraightOutput {
    public static void main(String[] args) throws Exception{
        // initialize the Flink execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        RandomSensorSource source = new RandomSensorSource(4, 10, 50);
        ParameterTool params = ParameterTool.fromArgs(args);
        int sensorNum = params.getInt("num", 2);
        int duration = params.getInt("duration", -1);
        int speed = params.getInt("speed", 1000);
        RandomSensorSource source = new RandomSensorSource(sensorNum, duration, speed);

        DataStream<Tuple2<Integer, Double>> dataStream = env
                .addSource(source);

        dataStream.keyBy(0)
                .print();
        /* wordcount
                .map(new MyMapper())
                .keyBy(0)
                .sum(1);
        */

        // create an output sink to print to stdout for verification
        // dataStream.print();

        // execute within the Flink environment
        env.execute("StraightOutput");
    }

    private static class MyMapper implements MapFunction<String, Tuple2<String, Integer>> {
        @Override
        public Tuple2<String, Integer> map(String s) {
            return Tuple2.of(s, 1);
        }
    }


    public static class MyApply implements WindowFunction<Tuple2<Integer, Double>, Tuple2<Integer, Long>, Tuple, GlobalWindow> {
        @Override
        public void apply(Tuple key, GlobalWindow window, Iterable<Tuple2<Integer, Double>> events, Collector<Tuple2<Integer, Long>> out) {
            long count = 0;
            Iterator<Tuple2<Integer, Double>> iter = events.iterator();
            while (iter.hasNext()) {
                count++;
                iter.next();
            }
            out.collect(Tuple2.of(((Tuple1<Integer>) key).f0, count));
        }
    }

}
