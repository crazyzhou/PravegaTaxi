package io.pravega.example.data;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

import java.io.Serializable;

/**
 * A TaxiRide is a taxi ride event. There are two types of events, a taxi ride start event and a
 * taxi ride end event. The isStart flag specifies the type of the event.
 *
 * A TaxiRide consists of
 * - the rideId of the event which is identical for start and end record
 * - the type of the event (start or end)
 * - the time of the event
 * - the longitude of the start location
 * - the latitude of the start location
 * - the longitude of the end location
 * - the latitude of the end location
 * - the passengerCnt of the ride
 * - the taxiId
 * - the driverId
 *
 */
public class TaxiRide implements Comparable<TaxiRide>, Serializable {

    private static transient DateTimeFormatter timeFormatter =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public TaxiRide() {
        this.startTime = LocalDateTime.now();
        this.endTime = LocalDateTime.now();
    }

    public TaxiRide(long rideId, boolean isStart, LocalDateTime startTime, LocalDateTime endTime,
                    float startLon, float startLat, float endLon, float endLat,
                    short passengerCnt, long taxiId, long driverId) {

        this.rideId = rideId;
        this.isStart = isStart;
        this.startTime = startTime;
        this.endTime = endTime;
        this.startLon = startLon;
        this.startLat = startLat;
        this.endLon = endLon;
        this.endLat = endLat;
        this.passengerCnt = passengerCnt;
        this.taxiId = taxiId;
        this.driverId = driverId;
    }

    public long rideId;
    public boolean isStart;
    public LocalDateTime startTime;
    public LocalDateTime endTime;
    public float startLon;
    public float startLat;
    public float endLon;
    public float endLat;
    public short passengerCnt;
    public long taxiId;
    public long driverId;

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(rideId).append(",");
        sb.append(isStart ? "START" : "END").append(",");
        sb.append(timeFormatter.format(startTime)).append(",");
        sb.append(timeFormatter.format(startTime)).append(",");
        sb.append(startLon).append(",");
        sb.append(startLat).append(",");
        sb.append(endLon).append(",");
        sb.append(endLat).append(",");
        sb.append(passengerCnt).append(",");
        sb.append(taxiId).append(",");
        sb.append(driverId);

        return sb.toString();
    }

    public static TaxiRide fromString(String line) {

        String[] tokens = line.split(",");
        if (tokens.length != 11) {
            throw new RuntimeException("Invalid record: " + line);
        }

        TaxiRide ride = new TaxiRide();

        try {
            ride.rideId = Long.parseLong(tokens[0]);

            switch (tokens[1]) {
                case "START":
                    ride.isStart = true;
                    ride.startTime = LocalDateTime.parse(tokens[2], timeFormatter);
                    ride.endTime = LocalDateTime.parse(tokens[3], timeFormatter);
                    break;
                case "END":
                    ride.isStart = false;
                    ride.endTime = LocalDateTime.parse(tokens[2], timeFormatter);
                    ride.startTime = LocalDateTime.parse(tokens[3], timeFormatter);
                    break;
                default:
                    throw new RuntimeException("Invalid record: " + line);
            }

            ride.startLon = tokens[4].length() > 0 ? Float.parseFloat(tokens[4]) : 0.0f;
            ride.startLat = tokens[5].length() > 0 ? Float.parseFloat(tokens[5]) : 0.0f;
            ride.endLon = tokens[6].length() > 0 ? Float.parseFloat(tokens[6]) : 0.0f;
            ride.endLat = tokens[7].length() > 0 ? Float.parseFloat(tokens[7]) : 0.0f;
            ride.passengerCnt = Short.parseShort(tokens[8]);
            ride.taxiId = Long.parseLong(tokens[9]);
            ride.driverId = Long.parseLong(tokens[10]);

        } catch (NumberFormatException nfe) {
            throw new RuntimeException("Invalid record: " + line, nfe);
        }

        return ride;
    }

    // sort by timestamp,
    // putting START events before END events if they have the same timestamp
    public int compareTo(TaxiRide other) {
        if (other == null) {
            return 1;
        }
        int compareTimes = Long.compare(this.getEventTime(), other.getEventTime());
        if (compareTimes == 0) {
            if (this.isStart == other.isStart) {
                return 0;
            }
            else {
                if (this.isStart) {
                    return -1;
                }
                else {
                    return 1;
                }
            }
        }
        else {
            return compareTimes;
        }
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof TaxiRide &&
                this.rideId == ((TaxiRide) other).rideId;
    }

    @Override
    public int hashCode() {
        return (int)this.rideId;
    }

    public long getEventTime() {
        if (isStart) {
            return startTime.atZone(ZoneId.of("EST")).toInstant().toEpochMilli();
        }
        else {
            return endTime.atZone(ZoneId.of("EST")).toInstant().toEpochMilli();
        }
    }

    public double getEuclideanDistance(double longitude, double latitude) {
        if (this.isStart) {
            return GeoUtils.getEuclideanDistance((float) longitude, (float) latitude, this.startLon, this.startLat);
        } else {
            return GeoUtils.getEuclideanDistance((float) longitude, (float) latitude, this.endLon, this.endLat);
        }
    }
}
