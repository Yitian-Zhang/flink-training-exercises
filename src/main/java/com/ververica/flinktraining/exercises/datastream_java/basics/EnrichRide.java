package com.ververica.flinktraining.exercises.datastream_java.basics;

import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.ververica.flinktraining.exercises.datastream_java.utils.GeoUtils;

/**
 * Custom Exercise from DOCS
 * Flink Stateless Transformations
 *
 * Based on the first exercise, there is a GeoUtils class that provides a static method
 * GeoUtils.mapToGridCell(float, lon, float lat) which maps a location (longgitude, latitude) to a
 * grid cell that refers to an area that is approximately 100 x 100 meters in size.
 *
 * @author yitian
 */
public class EnrichRide extends TaxiRide {

    public int startCell;
    public int endCell;

    public EnrichRide() {
    }

    public EnrichRide(TaxiRide ride) {
        this.rideId = ride.rideId;
        this.isStart = ride.isStart;
        this.startLat = ride.startLat;
        this.startLon = ride.startLon;
        this.endLat = ride.endLat;
        this.endLon = ride.endLon;
        this.taxiId = ride.taxiId;
        this.driverId = ride.driverId;
        this.startTime = ride.startTime;
        this.endTime = ride.endTime;
        this.passengerCnt = ride.passengerCnt;
        this.startCell = GeoUtils.mapToGridCell(ride.startLon, ride.startLat);

    }

    @Override
    public String toString() {
        return super.toString() + ", " +
                Integer.toString(this.startCell) + ", " +
                Integer.toString(this.endCell);
    }
}
