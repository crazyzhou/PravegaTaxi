package io.pravega.example.process;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class SocketWindowAverage {

    public static void main(String[] args) throws Exception{
        // initialize the Flink execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // average over a 5 second time period
        DataStream<Tuple2<Double, Double>> dataStream = env
                .socketTextStream("localhost", 9999).name("Socket Stream")
                .map(new MyMapper())
                .timeWindowAll(Time.seconds(5))
                .aggregate(new MyAgg(), new MyPro());

        // create an output sink to print to stdout for verification
        dataStream.print();

        // execute within the Flink environment
        env.execute("SocketWindowAverage");
    }

    private static class MyMapper implements MapFunction<String, Double> {

        @Override
        public Double map(String value) {
            double a;
            try {
                a = Double.parseDouble(value);
            } catch (NumberFormatException e) {
                return 0.0;
            }
            return a;
        }
    }

    private static class MyPro extends ProcessAllWindowFunction<Double, Tuple2<Double, Double>, TimeWindow> {
        private ValueState<Double> last;

        @Override
        public void open(Configuration config) throws Exception {
            ValueStateDescriptor<Double> descriptor = new ValueStateDescriptor<>("saved last", Double.class);
            last = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void process(Context context, Iterable<Double> elements, Collector<Tuple2<Double, Double>> out) throws Exception {
            for (Double d: elements) {
                out.collect(new Tuple2<>(d, last.value()));
                last.update(d);
            }
        }
    }

    private static class MyAgg implements AggregateFunction<Double, AverageAccumulator, Double> {

        @Override
        public AverageAccumulator createAccumulator() {
            return new AverageAccumulator(0, 0);
        }

        @Override
        public AverageAccumulator add(Double value, AverageAccumulator acc) {
            acc.sum += value;
            acc.count++;
            return acc;
        }

        @Override
        public Double getResult(AverageAccumulator acc) {
            return acc.sum / acc.count;
        }

        @Override
        public AverageAccumulator merge(AverageAccumulator a, AverageAccumulator b) {
            a.count += b.count;
            a.sum += b.sum;
            return a;
        }
    }

    public static class AverageAccumulator{
        int count;
        double sum;

        public AverageAccumulator(int count, double sum) {
            this.count = count;
            this.sum = sum;
        }
    }
}
