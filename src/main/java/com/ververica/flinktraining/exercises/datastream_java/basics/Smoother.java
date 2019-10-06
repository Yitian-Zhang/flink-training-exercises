package com.ververica.flinktraining.exercises.datastream_java.basics;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

public class Smoother extends RichMapFunction<Tuple2<String, Double>, Tuple2<String, Double>> {

    @Override
    public void open(Configuration parameters) throws Exception {

    }

    @Override
    public Tuple2<String, Double> map(Tuple2<String, Double> stringDoubleTuple2) throws Exception {
        return null;
    }
}
