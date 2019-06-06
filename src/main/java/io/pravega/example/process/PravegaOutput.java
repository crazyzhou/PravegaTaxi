package io.pravega.example.process;

import io.pravega.example.data.random.RandomSensorSource;
import io.pravega.example.data.random.RandomStringSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;

public class PravegaOutput {
    public static void main(String[] args) throws Exception {
        // initialize the Flink execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        RandomSensorSource source = new RandomSensorSource(4, 10, 50);
        ParameterTool params = ParameterTool.fromArgs(args);
        int duration = params.getInt("duration", 60);
        int speed = params.getInt("speed", 50);
        RandomStringSource source = new RandomStringSource(duration, speed);

        DataStream<String> dataStream = env.addSource(source);

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
}
