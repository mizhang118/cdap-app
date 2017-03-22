/**
 * @author Ming Zhang
 * created at 3/17/2017
 * 
 * general functions handle locations and time
 */

package com.ericsson.pm.c26.utilities;

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.TimeZone;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.pm.c26.common.C26AnalyticsConstants;

public class TimeLocationUtil {
	private static String[] dayNames = new String[] {"", "Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"};

	private static final Logger LOG = LoggerFactory.getLogger(TimeLocationUtil.class);
	
	/**
	 * Give an estimate of timezone by location's longitude
	 * 
	 * @param longitude
	 * @return
	 */
	public static TimeZone getTimeZoneByLongitude(double longitude) {
		 int offsetInHours = (int) (longitude / 15);
		 int offsetInMilliSeconds = offsetInHours * 3600 * 1000;
		 System.out.println(offsetInMilliSeconds);
		 String[] tzId = TimeZone.getAvailableIDs((int)offsetInMilliSeconds);
		 System.out.println(tzId + ", " + tzId.length);
		 if ( tzId == null || tzId.length == 0 ) {
			 return TimeZone.getTimeZone(C26AnalyticsConstants.VOLVO_TRIP_DATE_TIME_ZONE);
		 }
		 
		 String timezone = null;
		 for( String id : tzId ) {
			 //find the first 3-char timezone ID
			 if ( id.length() == 3 ) {
				 timezone = id;
				 break;
			 }
		 }
		 if ( timezone == null ) {
			 timezone = tzId[0];
		 }
		 return TimeZone.getTimeZone(timezone);
	}
	
	public static String getTimeOfDay(long timestamp, TimeZone timezone) {
		Date date = new Date(timestamp);
		Calendar cal = GregorianCalendar.getInstance();
		cal.setTimeZone(timezone);
		cal.setTime(date);
		int hour = cal.get(Calendar.HOUR_OF_DAY);
		if (hour > 21 || hour < 6) return "Night";
		if (hour < 9)  return "Morning";
		if (hour < 12) return "Late Morning";
		if (hour < 14) return "Afternoon";
		if (hour < 16) return "Late Afternoon";
		if (hour < 19) return "Evening";
		return "Late Evening";
	}
	
	public static String getDayOfWeek(long timestamp, TimeZone timezone) {
		Date date = new Date(timestamp);
		Calendar cal = GregorianCalendar.getInstance();
		cal.setTimeZone(timezone);
		cal.setTime(date);
		return dayNames[cal.get(Calendar.DAY_OF_WEEK)];
	}
	
	public static String getDayType(long timestamp, TimeZone timezone) {
		Date date = new Date(timestamp);
		Calendar cal = GregorianCalendar.getInstance();
		cal.setTimeZone(timezone);
		cal.setTime(date);
		int dow = cal.get(Calendar.DAY_OF_WEEK);
		if ( dow > 1 && dow < 7) return "weekday";
		return "weekend";
	}

}
