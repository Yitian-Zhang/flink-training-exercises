package com.ververica.flinktraining.exercises.datastream_java.basics.transform;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * Docs: Connected Streams
 * In this example a control stream is used to specify words which must be filtered out of the
 * streamOfWords.
 * A RichCoFlatMapFunction called ControllFunction is applied to the connected streams to get this done.
 */
public class ConnectedStream {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> contorl = env.fromElements("DROP", "IGNORE").keyBy(x -> x);
        DataStream<String> streamOfWords = env.fromElements("data", "DROP", "artisans", "IGNORE")
                .keyBy(x -> x);

        /**
         * flatMap1 and flatMap2 are called by the Flink runtime with elements from each of the two connected streams –
         * in our case, elements from the control stream are passed into flatMap1,
         * and elements from streamOfWords are passed into flatMap2.
         *
         * This was determined by the order in which we connected the two streams via control.connect(datastreamOfWords).
         */
        contorl.connect(streamOfWords)
                .flatMap(new ControlFunction())
                .print();

        env.execute();
    }

    /**
     * Note that the two streams being connected must be keyed in compatible ways – either both streams are not keyed,
     * or both are keyed, and if they are both keyed, the key values have to be the same.
     *
     * In this case the streams are both of type DataStream<String>, and both streams are keyed by the string.
     * As you will see below, this RichCoFlatmap is storing a Boolean value in keyed state,
     * and this Boolean is shared by the two streams.
     */
    public static class ControlFunction extends RichCoFlatMapFunction<String, String, String> {

        private ValueState<Boolean> blocked;

        @Override
        public void open(Configuration parameters) throws Exception {
            blocked = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("blocked", Boolean.class));

        }

        @Override
        public void flatMap1(String s, Collector<String> collector) throws Exception {
            blocked.update(Boolean.TRUE);
        }

        @Override
        public void flatMap2(String s, Collector<String> collector) throws Exception {
            if (blocked.value() == null) {
                collector.collect(s);
            }
        }
    }
}
