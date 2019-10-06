package com.ververica.flinktraining.exercises.datastream_java.basics;

import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.joda.time.Interval;
import org.joda.time.Minutes;

import static com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase.rideSourceOrTest;

/**
 * Keyed Stream
 * RideCleansingExercise expand.
 *
 * Expand 1: There is a GeoUtils class that provides a static method
 * GeoUtils.mapToGridCell(float lon, float lat) which maps a location (longitude, latitude)
 * to a grid cell that refers to an area that is approximately 100x100 meters in size.
 *
 * Expand 2: A MapFunction is suitable only when performing a one-to-one transformation:
 * for each and every stream element coming in, map() will emit one transformed element.
 * Otherwise, you’ll want to use flatmap()
 *
 * Expand 3: Suppose we wanted to find the longest taxi rides starting in each of the grid cells.
 * This is the first example we’ve seen of stateful streaming.
 * Though the state is being handled transparently,
 * Flink is having to keep track of the maximum duration for each distinct key.
 *
 *
 *
 * @author yitian
 */
public class RideCleansingExpand {

    public static void main(String[] args) throws Exception {

        ParameterTool params = ParameterTool.fromArgs(args);
        final String input = params.get("input", ExerciseBase.pathToRideData);

        final int maxEventDelay = 60;       // events are out of order by max 60 seconds
        final int servingSpeedFactor = 600; // events of 10 minutes are served in 1 second

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(ExerciseBase.parallelism);

        // start the data generator
        DataStream<TaxiRide> rides = env.addSource(rideSourceOrTest(new TaxiRideSource(input, maxEventDelay, servingSpeedFactor)));

		/*
		Custom expand 1:
		使用map方法扩展代码
		 */
        DataStream<EnrichRide> enrichRides = rides
                .filter(new RideCleansingExercise.NYCFilter())
                .map(new Enrichment());
        enrichRides.print();

		/*
		Custom expand 2:
		使用flatmap扩展代码
		A MapFunction is suitable only when performing a one-to-one transformation: for each and every
			stream element coming in, map() will emit one transformed element.
			Otherwise, you'll want to use flatMap()
		 */
        DataStream<EnrichRide> enrichRideData = rides.flatMap(new NYCEnrichment());
		enrichRideData.print();

		/*
		Custom expand 3:
		Using keyBy to partition the stream
		rides.flatMap(new NYCEnrichment())
				.keyBy("startCell");
		rides.flatMap(new NYCEnrichment())
				.keyBy(ride -> ride.startCell);

		Attention:
		The key has been specified by its name, “startCell”.
		 This style of key selection has the drawback that the compiler is unable to infer the type of the
		 field being used for keying, and so Flink will pass around the key values as Tuples, which can be awkward.
		 It is generally preferable to use a properly typed KeySelector.
		 */

        DataStream<Tuple2<Integer, Minutes>> minutesByStartCell = enrichRideData
                .flatMap(new FlatMapFunction<EnrichRide, Tuple2<Integer, Minutes>>() {
                    @Override
                    public void flatMap(EnrichRide enrichRide, Collector<Tuple2<Integer, Minutes>> collector) throws Exception {
                        if (!enrichRide.isStart) {
                            Interval rideInterval = new Interval(enrichRide.startTime, enrichRide.endTime);
                            Minutes duration = rideInterval.toDuration().toStandardMinutes();
                            collector.collect(new Tuple2<>(enrichRide.startCell, duration));
                        }
                    }
                });

        // We are now in a position to produce a stream that contains only those rides that are the longest rides
        // ever seen (to that point) for each startCell.
        minutesByStartCell.keyBy(0)
                .maxBy(1)
                .print();

        // run the cleansing pipeline
        env.execute("Taxi Ride Cleansing");
    }


    public static class Enrichment implements MapFunction<TaxiRide, EnrichRide> {
        @Override
        public EnrichRide map(TaxiRide taxiRide) throws Exception {
            return new EnrichRide(taxiRide);
        }
    }

    public static class NYCEnrichment implements FlatMapFunction<TaxiRide, EnrichRide> {

        @Override
        public void flatMap(TaxiRide taxiRide, Collector<EnrichRide> collector) throws Exception {
            FilterFunction<TaxiRide> vaild = new RideCleansingExercise.NYCFilter();
            if (vaild.filter(taxiRide)) {
                collector.collect(new EnrichRide(taxiRide));
            }
        }
    }
}
