package com.ververica.flinktraining.exercises.datastream_java.basics;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

/**
 * Stateful transformation
 * TODO: this class is unused.
 *
 * @author yitian
 */
public class Smoother extends RichMapFunction<Tuple2<String, Double>, Tuple2<String, Double>> {

    private ValueState<MovingAverage> averageState;

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<MovingAverage> descriptor =
                new ValueStateDescriptor<MovingAverage>("moving average", MovingAverage.class);
        averageState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public Tuple2<String, Double> map(Tuple2<String, Double> item) throws Exception {
        // access the state for this key
        MovingAverage average = averageState.value();

        // create a new MovingAverage (with window size 2) if none exists for this key
        if (average == null) {
            average = new MovingAverage(2);
        }

        // add this event to the moving average
        average.add(item.f1);
        averageState.update(average);


        // return the smoothed result
        return new Tuple2<>(item.f0, average.getAverage());
    }

    /**
     * Don't know the structure of this custom class.
     *
     * @author yitian
     */
    public class MovingAverage {

        private Double average;
        private Integer window;

        public MovingAverage(int window) {
            this.window = window;
        }

        public void add(Double d) {

        }

        public Double getAverage() {
            return average;
        }

        public void setAverage(Double average) {
            this.average = average;
        }
    }
}
