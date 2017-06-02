/**
 * @author Ming Zhang
 * created at 3/17/2017
 * 
 * Extract useful data fields from volvo raw trip data
 */

package com.ericsson.pm.c26.entities;

import com.ericsson.pm.c26.common.C26AnalyticsConstants;
import com.ericsson.pm.c26.utilities.TimeLocationUtil;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

import javax.annotation.Nullable;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VolvoTrip extends Entity implements WritableComparable<VolvoTrip> {
	private static final long serialVersionUID = 2L;

	private static final Logger LOG = LoggerFactory.getLogger(VolvoTrip.class);
	
	private String id;
	private Double endLatitude;
	private Double endLongitude;
	private String endStreetAddress;
	private String endCity;
	private String endRegion;
	private Long endTime;
	private double startLatitude;
	private double startLongitude;
	private String startStreetAddress;
	private String startCity;
	private String startRegion;
	private Long startTime;
	private String vin;
	private Long parkingTime;
	
	public VolvoTrip() {
		super();
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public Double getEndLatitude() {
		return endLatitude;
	}

	public void setEndLatitude(Double endLatitude) {
		this.endLatitude = endLatitude;
	}

	public Double getEndLongitude() {
		return endLongitude;
	}

	public void setEndLongitude(Double endLongitude) {
		this.endLongitude = endLongitude;
	}

	public String getEndStreetAddress() {
		return endStreetAddress;
	}
	
	public void setEndStreetAddress(String endStreetAddress) {
		this.endStreetAddress = endStreetAddress;
	}

	public String getEndCity() {
		return endCity;
	}

	public void setEndCity(String endCity) {
		this.endCity = endCity;
	}

	public String getEndRegion() {
		return endRegion;
	}

	public void setEndRegion(String endRegion) {
		this.endRegion = endRegion;
	}

	public Long getEndTime() {
		return endTime;
	}

	public void setEndTime(Long endTime) {
		this.endTime = endTime;
	}

	public double getStartLatitude() {
		return startLatitude;
	}

	public void setStartLatitude(double startLatitude) {
		this.startLatitude = startLatitude;
	}

	public double getStartLongitude() {
		return startLongitude;
	}

	public void setStartLongitude(double startLongitude) {
		this.startLongitude = startLongitude;
	}

	public String getStartStreetAddress() {
		return startStreetAddress;
	}

	public void setStartStreetAddress(String startStreetAddress) {
		this.startStreetAddress = startStreetAddress;
	}

	public String getStartCity() {
		return startCity;
	}

	public void setStartCity(String startCity) {
		this.startCity = startCity;
	}

	public String getStartRegion() {
		return startRegion;
	}

	public void setStartRegion(String startRegion) {
		this.startRegion = startRegion;
	}

	public Long getStartTime() {
		return startTime;
	}

	public void setStartTime(Long startTime) {
		this.startTime = startTime;
	}

	public String getVin() {
		return vin;
	}

	public void setVin(String vin) {
		this.vin = vin;
	}

	public Long getParkingTime() {
		return parkingTime;
	}

	public void setParkingTime(Long parkingTime) {
		this.parkingTime = parkingTime;
	}

	@Nullable
	public static VolvoTrip parse(String input) {
		if ( input == null ) {
			return null;
		}
		
		String[] fields = input.split(",");

		if ( !(fields.length == C26AnalyticsConstants.VOLVO_TRIP_DATA_FIELD_COUNT ) ) {
			LOG.error("The input data has wrong number of data fields: {}", fields.length);
			return null;
		}
/**************************************
37 data fields

0 String id;
1 String endCity;
2 String endIsO2CountryCode;
3 double endLatitude;
4 double endLongitude;
5 double endOdometer;
6 double endPostalCode;
7 String endRegion;
8 String endStreetAddress;
9 String endTime;
10 double fuelConsumption;
11 String startCity;
12 String startIsO2CountryCode;
13 double startLatitude;
14 double startLongitude;
15 double startOdometer;
16 double startPostalCode;
17 String startRegion;
18 String startStreetAddress;
19 String startTime;
20 String trip_id;
21 double endOdometerDerived;
22 double endPositionDerived;
23 double startOdometerDerived;
24 double startPositionDerived;
25 double electricalConsumption;
26 double electricalRegeneration;
27 double tripwayPointsMinLatitude;
28 double tripwayPointsMinLongitude;
29 double tripwayPointsMaxLatitude;
30 double tripwayPointsMaxLongitude;
31 double tripwayPointsNBrof;
32 String id_1;
33 String Category;
34 String endTime_1;
35 String startTime_1;
36 String vehicle_id;		
****************************************/
		VolvoTrip trip = new VolvoTrip();
		try {
			trip.setId(fields[0].trim());
			trip.setVin(fields[36].trim());
			trip.setEndLatitude(new Double(fields[3].trim()));
			trip.setEndLongitude(new Double(fields[4].trim()));
			trip.setEndStreetAddress(fields[8].trim());
			trip.setEndCity(fields[1].trim());
			trip.setEndRegion(fields[7]);
			trip.setEndTime(parseTime(fields[9].trim(), trip.endLongitude));
			
			trip.setStartLatitude(new Double(fields[13].trim()));
			trip.setStartLongitude(new Double(fields[14].trim()));
			trip.setStartStreetAddress(fields[18].trim());
			trip.setStartCity(fields[11].trim());
			trip.setStartRegion(fields[17].trim());
			trip.setStartTime(parseTime(fields[19].trim(), trip.startLongitude));
		}
		catch (Exception e) {
			LOG.error("Wrong input data format: {}", input, e);
			trip = null;
		}
		
		return trip;
	}
	
	private static long parseTime(String time, Double longitude) throws ParseException {
		SimpleDateFormat dateFormat = new SimpleDateFormat(C26AnalyticsConstants.VOLVO_TRIP_DATE_TIME_FORMAT);
		TimeZone tz = TimeLocationUtil.getTimeZoneByLongitude(longitude);
		dateFormat.setTimeZone(tz);
		return dateFormat.parse(time).getTime();
	}
	
	
	  @Override
	  public int compareTo(VolvoTrip trip) {
		  String thisJson = this.toJson();
		  
		  return thisJson.compareTo(trip.toJson());
	  }

	  @Override
	  public void write(DataOutput dataOutput) throws IOException {
		  WritableUtils.writeString(dataOutput, this.toJson());
	  }

	  @Override
	  public void readFields(DataInput dataInput) throws IOException {
		  String json = WritableUtils.readString(dataInput);
		  VolvoTrip trip = (VolvoTrip) (new VolvoTrip()).toEntity(json);
		  this.copy(trip);
	  }
}
