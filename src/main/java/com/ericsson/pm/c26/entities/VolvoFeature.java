/**
 * @author Ming Zhang
 * created at 3/21/2017
 * 
 * Extract features from VolvoTrip data fields
 */

package com.ericsson.pm.c26.entities;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.TimeZone;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.pm.c26.utilities.TimeLocationUtil;

public class VolvoFeature extends Entity implements Writable, Comparable<VolvoFeature> {
	private static final long serialVersionUID = 3L;
	
	private static final Logger LOG = LoggerFactory.getLogger(VolvoFeature.class);
	
	private String id;
	private String vehicleId;
	private String origin;
	private String destination;
	private String duration;
	private String dayOfWeek;
	private String timeOfDay;
	private String dayType;
	
	private Double startLon;
	private Double startLat;
	private Double endLon;
	private Double endLat;
	
	private Long startTime;
	private Long endTime;
	
	public VolvoFeature() {
		super();
	}
	
	public VolvoFeature(String json) {
		super(json);
	}
	
	public VolvoFeature(VolvoTrip trip) {
		this.setId(trip.getId());
		this.setVehicleId(trip.getVin());
		this.startTime = trip.getStartTime();
		this.endTime = trip.getEndTime();
		TimeZone originTimezone = TimeLocationUtil.getTimeZoneByLongitude(trip.getStartLongitude());
		this.setDayOfWeek(TimeLocationUtil.getDayOfWeek(trip.getStartTime(), originTimezone));
		this.setTimeOfDay(TimeLocationUtil.getTimeOfDay(trip.getStartTime(), originTimezone));
		this.setDayType(TimeLocationUtil.getDayType(trip.getStartTime(), originTimezone));
		this.setOrigin(filterStreetNumber(trip.getStartStreetAddress()) + " " + trip.getStartCity() + " " + trip.getStartRegion());
		this.setDestination(trip.getEndStreetAddress() + " " + trip.getEndCity() + " " + trip.getEndRegion());
		
		this.setStartLat(trip.getStartLatitude());
		this.setStartLon(trip.getStartLongitude());
		this.setEndLat(trip.getEndLatitude());
		this.setEndLon(trip.getEndLongitude());
	}
	
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getVehicleId() {
		return vehicleId;
	}

	public void setVehicleId(String vehicleId) {
		this.vehicleId = vehicleId;
	}

	public String getOrigin() {
		return origin;
	}

	public void setOrigin(String origin) {
		this.origin = origin;
	}

	public String getDestination() {
		return destination;
	}

	public void setDestination(String destination) {
		this.destination = destination;
	}

	public String getDuration() {
		return duration;
	}

	public void setDuration(String duration) {
		this.duration = duration;
	}

	public String getDayOfWeek() {
		return dayOfWeek;
	}

	public void setDayOfWeek(String dayOfWeek) {
		this.dayOfWeek = dayOfWeek;
	}

	public String getTimeOfDay() {
		return timeOfDay;
	}

	public void setTimeOfDay(String timeOfDay) {
		this.timeOfDay = timeOfDay;
	}

	public String getDayType() {
		return dayType;
	}

	public void setDayType(String dayType) {
		this.dayType = dayType;
	}

	public Double getStartLon() {
		return startLon;
	}

	public void setStartLon(Double startLon) {
		this.startLon = startLon;
	}

	public Double getStartLat() {
		return startLat;
	}

	public void setStartLat(Double startLat) {
		this.startLat = startLat;
	}

	public Double getEndLon() {
		return endLon;
	}

	public void setEndLon(Double endLon) {
		this.endLon = endLon;
	}

	public Double getEndLat() {
		return endLat;
	}

	public void setEndLat(Double endLat) {
		this.endLat = endLat;
	}

	public Long getStartTime() {
		return startTime;
	}

	public void setStartTime(Long startTime) {
		this.startTime = startTime;
	}

	public Long getEndTime() {
		return endTime;
	}

	public void setEndTime(Long endTime) {
		this.endTime = endTime;
	}

	/**
	 * @param input string with format of MorningsideDrive176, supposing street number is in the end.
	 * @return
	 */
	private static String filterStreetNumber(String input) {
		if ( input == null ) {
			return null;
		}
		
		int idx = input.length() - 1;
		while ( idx >= 0 ) {
			char ch = input.charAt(idx);
			if ( Character.isLetter(ch) ) {
				break;
			}
			
			idx--;
		}
		
		idx++;
		return input.substring(0, idx);
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		WritableUtils.writeString(dataOutput, this.toJson());
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		String json = WritableUtils.readString(dataInput);
		VolvoTrip trip = new VolvoTrip(json);
		this.copy(trip);
	}

	@Override
	public int compareTo(VolvoFeature feature) {
		if ( feature.startTime == null && this.startTime != null ) {
			return 1;
		}
		else if ( feature.startTime != null && this.startTime == null ) {
			return -1;
		}
		else if ( feature.startTime == null && this.startTime == null ) {
			return 0;
		}
		
		if ( this.startTime > feature.startTime ) {
			return 1;
		}
		else if ( this.startTime < feature.startTime ) {
			return -1;
		}
		
		return 0;
	}

	public String asSparkFriendlyFeatureVector() {
	return "origin=" + getOrigin() + "," + "destination=" + getDestination() + "," + "timeOfDay=" + getTimeOfDay() + "," + "dayOfWeek=" +
	       getDayOfWeek() + "," + "dayType=" + getDayType() + "," + "duration=" + getDuration();
	}
	
	public String asRFriendlyFeatureVector() {
	return this.getVehicleId() + "," + getOrigin() + "," + getDestination() + "," + getTimeOfDay() + "," + 
	       getDayOfWeek() + "," + getDayType() + "," + getDuration();
	}
	
	public String asClassificationFeatureVector() {
	return this.getVehicleId() + "," + this.getStartLat() + "," + this.getStartLon() + "," + this.getEndLat() + "," + this.getEndLon() + "," + getTimeOfDay() + "," + 
	       getDayOfWeek() + "," + getDayType() + "," + getDuration();
	}
	
	public static String getRFriendlyFeatureVectorHeaders() {
		return "origin,destination,timeOfDay,dayOfWeek,dayType,duration";
	}
}
