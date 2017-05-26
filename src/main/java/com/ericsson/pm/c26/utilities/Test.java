package com.ericsson.pm.c26.utilities;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.ericsson.pm.c26.entities.VolvoFeature;
import com.ericsson.pm.c26.entities.VolvoTrip;

public class Test {
	private Map<String, Long> endTimeOfLastTrip = new HashMap<String, Long>(2048);
	
	private static final String[] TIME_IN_A_DAY = {"Morning","LateMorning","Afternoon","LateAfternoon","Evening","LateEvening","Night"};
	private static final String[] DAY_IN_A_WEEK = {"Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"};
	
	public static void main(String[] args) {
		Test t = new Test();
		//t.generateFeatures();
		//t.classificationFeatures();
		//t.simpleClassificationFeatures();
		//t.testData();
		t.convertToClassificationFeatures2();
	}
	
	public void testData() {
		try {
			BufferedReader reader = new BufferedReader(new FileReader("/Users/mz/test/R/volvo/results.txt"));
			FileWriter writer = new FileWriter(new File("/Users/mz/test/R/volvo/verify.txt"));
			String line = reader.readLine(); //read header
			line += ",\"Distance\"\n";
			writer.write(line);
			while( (line = reader.readLine()) != null ) {
				line = line.trim();
				String[] fields = line.split(",");
				double lat1 = Double.parseDouble(fields[0]);
				double lat2 = Double.parseDouble(fields[1]);
				double lon1 = Double.parseDouble(fields[2]);
				double lon2 = Double.parseDouble(fields[3]);
				
				double distance = distFrom(lat1, lon1, lat2, lon2);
				line += "," + distance + "\n";
				writer.write(line );
			}
			writer.close();
			reader.close();
		}
		catch (Exception e) {
			e.printStackTrace(System.err);
		}		
		
	}
	
	public Integer find(String[] strings, String findMe) {
		for ( int i = 0; i < strings.length; i++ ) {
			if ( strings[i].equals(findMe) ) {
				return i;
			}
		}
		
		System.out.println(Arrays.toString(strings) + " <-" + findMe);
		return -1;
	}
	
	public void add(Map<String, Integer> map, String key) {
		Integer value = map.get(key);
		if ( value == null ) {
			map.put(key,  1);
		}
		else {
			value = value + 1;
			map.put(key, value);
		}
	}
	
	public void convertToClassificationFeatures2() {
		try {
			BufferedReader reader = new BufferedReader(new FileReader("/Users/mz/test/volvo/data/features.txt"));
			File dir = new File("/Users/mz/test/volvo/data/features");
			String line = null;
			String previousVin = null;
			ArrayList<String[]> data = new ArrayList<String[]>();
			Map<String, Integer> origins = new HashMap<String, Integer>();
			Map<String, Integer> destinations = new HashMap<String, Integer>();
			while( (line = reader.readLine()) != null ) {
				line = line.trim();
				String[] fields = line.split(",");
				String vin = fields[0];
				
				if ( previousVin == null) {
					previousVin = vin;
				}
				
				if ( vin.equals(previousVin) ) {
					data.add(fields);
					if ( fields[1].length() > 3 ) {
						add(origins, fields[1]);
					}
					if ( fields[2].length() > 3 ) {
						add(destinations, fields[2]);
					}
				}
				else {
					try {
						String vehicleId = data.get(0)[0];
						//generate classification features of this vin
						FileWriter writer = new FileWriter(new File(dir, vehicleId));
						
						//classification features are:
						//index 0: origin
						//index 1: time 0-6: Morning, Late Morning, Afternoon, Late Afternoon, Evening, Late Evening, Night
						//index 2: Weekday: 1, Weekday; 0, Weekend
						//index 3: 0-6: Monday, Tuesday, Wednesday, Thursday, Friday, Saturday, Sunday
						//index 4: destination
						//index 5: parking
						
						//write classification feature data
						origins = MapUtil.sortByValue(origins);
						Map<String, Integer> originValue = new HashMap<String, Integer>();
						int count = 1;
						for ( Map.Entry<String, Integer> entry : origins.entrySet() ) {
							originValue.put(entry.getKey(), count++);
						}
						destinations = MapUtil.sortByValue(destinations);
						Map<String, Integer> destValue = new HashMap<String, Integer>();
						count = 1;
						for ( Map.Entry<String, Integer> entry : destinations.entrySet() ) {
							destValue.put(entry.getKey(), count++);
						}
						//write title
						writer.write("origin,time,weekend,date,destination,parking\n");
						for(String[] attr : data ) {
							StringBuffer buffer = new StringBuffer();
							String ori = attr[1];
							String dest = attr[2];
							String time = attr[3].replaceAll(" ", "");
							String date = attr[4];
							String weekend = attr[5];
							String parking = attr[6];
							Integer ov = originValue.get(ori);
							if ( ov == null ) {
								continue;
							}
							buffer.append(ov + ",");
							//buffer.append(ori + ",");
							buffer.append(find(TIME_IN_A_DAY, time) + ",");
							//buffer.append(time + ",");
							
							int weekendint = 1;
							if ( weekend.equals("weekend") ) {
								weekendint = 0;
							}
							buffer.append(weekendint + ",");
							
							//buffer.append(weekend + ",");
							buffer.append(find(DAY_IN_A_WEEK, date) + ",");
							//buffer.append(date + ",");
							
							Integer dv = destValue.get(dest);
							if ( dv == null ) {
								continue;
							}
							buffer.append(dv + ",");
							
							//buffer.append(dest + ",");
							buffer.append(parking + "\n");
							writer.write(buffer.toString());
						}
						writer.close();
					}
					catch (Exception e) {
						e.printStackTrace(System.err);
					}
					
					previousVin = vin;
					data = null;
					data = new ArrayList<String[]>();
					data.add(fields);
					
					origins = null;
					origins = new HashMap<String, Integer>();
					if ( fields[1].length() > 3 ) {
						add(origins, fields[1]);
					}
					destinations = null;
					destinations = new HashMap<String, Integer>();
					if ( fields[2].length() > 3 ) {
						add(destinations, fields[2]);
					}
				}
			}
			
			reader.close();
		}
		catch (Exception e) {
			e.printStackTrace(System.err);
		}
	}
	
	public void simpleClassificationFeatures() {
		try {
			BufferedReader reader = new BufferedReader(new FileReader("/Users/mz/test/volvo/data/features.txt"));
			File dir = new File("/Users/mz/test/volvo/data/features");
			String line = null;
			String previousVin = null;
			ArrayList<String[]> data = new ArrayList<String[]>();
			while( (line = reader.readLine()) != null ) {
				line = line.trim();
				String[] fields = line.split(",");
				String vin = fields[0];
				
				if ( previousVin == null) {
					previousVin = vin;
				}
				
				if ( vin.equals(previousVin) ) {
					data.add(fields);
				}
				else {
					try {
						String vehicleId = data.get(0)[0];
						//generate classification features of this vin
						FileWriter writer = new FileWriter(new File(dir, vehicleId));
						
						//classification features are:
						//index 0 - 3: origin lat, origin lon, dest lat, dest lon 
						//index 4: parking,
						//index 5: Time: 0 - 6 (Morning, Late Morning, Afternoon, Late Afternoon, Evening, Late Evening, Night)
						//index 12: Weekday: 1 weekday, 0 weekend
						
						//write title
						String tit = "OriginLat,OriginLon,DestLat,DestLon,Parking,Time,Weekday";
						writer.write(tit + "\n");
						
						//write classification feature data
						for(String[] attr : data ) {
							writer.write(attr[1] + "," + attr[2] + ","+ attr[3] + ","+ attr[4]);
							// fill parking m
							writer.write("," + attr[8]);

							// fill time: 5 - 11
							String time = attr[5].replaceAll(" ", "");
							int t = getTime(time);
							writer.write("," + t);
							
							// fill weekday/weekend: m+8, m+9
							String week = attr[7];
							if ( week.equals("weekday") ) {
								writer.write(",1");
							}
							else {
								writer.write(",0");
							}
							writer.write("\n");
						}
						writer.close();
					}
					catch (Exception e) {
						e.printStackTrace(System.err);
					}
					
					previousVin = vin;
					data = null;
					data = new ArrayList<String[]>();
					data.add(fields);
				}
			}
			
			reader.close();
		}
		catch (Exception e) {
			e.printStackTrace(System.err);
		}
	}
	
	public void classificationFeatures() {
		try {
			BufferedReader reader = new BufferedReader(new FileReader("/Users/mz/test/volvo/data/features.txt"));
			File dir = new File("/Users/mz/test/volvo/data/features");
			String line = null;
			String previousVin = null;
			ArrayList<String[]> data = new ArrayList<String[]>();
			while( (line = reader.readLine()) != null ) {
				line = line.trim();
				String[] fields = line.split(",");
				String vin = fields[0];
				
				if ( previousVin == null) {
					previousVin = vin;
				}
				
				if ( vin.equals(previousVin) ) {
					data.add(fields);
				}
				else {
					try {
						String vehicleId = data.get(0)[0];
						//generate classification features of this vin
						FileWriter writer = new FileWriter(new File(dir, vehicleId));
						
						//classification features are:
						//index 0 - 3: origin lat, origin lon, dest lat, dest lon 
						//index 4: parking,
						//index 5 - 11: Morning, Late Morning, Afternoon, Late Afternoon, Evening, Late Evening, Night
						//index 12: Weekday
						//index 13 - 19: Sunday, Monday, Tuesday, Wednesday, Thursday, Friday, Saturday
						
						//write title
						String tit = "OriginLat,OriginLon,DestLat,DestLon,ParkingTime,Morning,LateMorning,Afternoon,LateAfternoon,Evening,LateEvening,Night,Weekday,Sun,Mon,Tue,Wed,Thu,Fri,Sat";
						writer.write(tit + "\n");
						String[] title = tit.split(",");
						
						//write classification feature data
						for(String[] attr : data ) {
							writer.write(attr[1] + "," + attr[2] + ","+ attr[3] + ","+ attr[4]);
							// fill parking m
							writer.write("," + attr[8]);

							// fill time: 5 - 11
							String time = attr[5].replaceAll(" ", "");
							for ( int j = 5; j <= 11; j++ ) {
								
								if ( time.equals(title[j]) ) {
									writer.write(",1");
								}
								else {
									writer.write(",0");
								}
							}
							
							// fill weekday/weekend: m+8, m+9
							String week = attr[7];
							if ( week.equals("weekday") ) {
								writer.write(",1");
							}
							else {
								writer.write(",0");
							}
							
							//fill date: m+10 - m+16
							String date = attr[6].substring(0, 3);
							for ( int j = 13; j <= 19; j++ ) {
								
								if ( date.equals(title[j]) ) {
									writer.write(",1");
								}
								else {
									writer.write(",-1");
								}
							}
							writer.write("\n");
						}
						writer.close();
					}
					catch (Exception e) {
						e.printStackTrace(System.err);
					}
					
					previousVin = vin;
					data = null;
					data = new ArrayList<String[]>();
					data.add(fields);
				}
			}
			
			reader.close();
		}
		catch (Exception e) {
			e.printStackTrace(System.err);
		}
	}
	
	public void convertToClassificationFeatures() {
		try {
			BufferedReader reader = new BufferedReader(new FileReader("/Users/mz/test/volvo/data/features.txt"));
			File dir = new File("/Users/mz/test/volvo/data/features");
			String line = null;
			String previousVin = null;
			ArrayList<String[]> data = new ArrayList<String[]>();
			Map<String, String> origins = new HashMap<String, String>();
			Map<String, String> destinations = new HashMap<String, String>();
			while( (line = reader.readLine()) != null ) {
				line = line.trim();
				String[] fields = line.split(",");
				String vin = fields[0];
				
				if ( previousVin == null) {
					previousVin = vin;
				}
				
				if ( vin.equals(previousVin) ) {
					data.add(fields);
					if ( fields[1].length() > 3 ) {
						origins.put(fields[1], "");
					}
					if ( fields[2].length() > 3 ) {
						destinations.put(fields[2], "");
					}
				}
				else {
					try {
						String vehicleId = data.get(0)[0];
						//generate classification features of this vin
						FileWriter writer = new FileWriter(new File(dir, vehicleId));
						FileWriter writer2 = new FileWriter(new File(dir, vehicleId + "_address"));
						
						//classification features are:
						//index 0 - (m-1): origin_1, origin_2, ..., origin_m, 
						//index m: parking,
						//index m+1 - m+7: Morning, Late Morning, Afternoon, Late Afternoon, Evening, Late Evening, Night
						//index m+8 - m+9: weekday, weekend
						//index m+10 - m+16: Sunday, Monday, Tuesday, Wednesday, Thursday, Friday, Saturday
						//index m+17 + m+17+(n-1) - add n: destination_1, destination_2, destination_n
						//Total_feature_number=m+n+17; index: 0 - (m+n+16)
						
						//at first generate titile:
						int m = origins.size();
						int n = destinations.size();
						String[] title = new String[m+n+17];
						Set<String> originSet = origins.keySet();
						//System.out.println(originSet);
						int index = 0;
						Map<String, String> oriMap = new HashMap<String, String>();
						for ( String origin : originSet ){
							String oriNoSpace = origin.replaceAll(" ", "");
							title[index++] = oriNoSpace;
							oriMap.put(oriNoSpace, "S" + index);
							writer2.write("S" + index + "=" + oriNoSpace + "\n");
						}
						title[index++] = "parking";
						title[index++] = "Morning";
						title[index++] = "LateMorning";
						title[index++] = "Afternoon";
						title[index++] = "LateAfternoon";
						title[index++] = "Evening";
						title[index++] = "LateEvening";
						title[index++] = "Night";
						title[index++] = "weekday";
						title[index++] = "weekend";
						title[index++] = "Sunday";
						title[index++] = "Monday";
						title[index++] = "Tuesday";
						title[index++] = "Wednesday";
						title[index++] = "Thursday";
						title[index++] = "Friday";
						title[index++] = "Saturday";
						Set<String> destinationSet = destinations.keySet();
						int destNum = 1;
						Map<String, String> destMap = new HashMap<String, String>();
						for ( String dest : destinationSet ){
							String destNoSpace = dest.replaceAll(" ", "");
							title[index++] = destNoSpace;
							
							destMap.put(destNoSpace, "E" + destNum);
							writer2.write("E" + destNum + "=" + destNoSpace + "\n");
							destNum++;
						}
						
						//write title
						int i = 0;
						for( String s : title ) {
							if ( i > 0 ) {
								writer.write(",");
							}
							if ( i < m ) {
								s = oriMap.get(s);
							}
							else if ( i >= m+17) {
								s = destMap.get(s);
							}
							
							writer.write(s);
							i++;
						}
						writer.write("\n");
						
						//write classification feature data
						String[] fea = new String[m+n+17];
						for(String[] attr : data ) {
							// fill origins [0 - (m-1)]
							for ( int j = 0; j < m; j++ ) {
								String ori = attr[1].replace(" ", "");
								if ( j > 0 ) {
									writer.write(",");
								}
								if ( ori.equals(title[j]) ) {
									writer.write("1");
								}
								else {
									writer.write("-1");
								}
							}
							
							// fill parking m
							writer.write("," + attr[6]);
							
							// fill time: m+1 - m+7
							for ( int j = m+1; j <= m+7; j++ ) {
								String time = attr[3].replaceAll(" ", "");
								if ( time.equals(title[j]) ) {
									writer.write(",1");
								}
								else {
									writer.write(",-1");
								}
							}
							
							// fill weekday/weekend: m+8, m+9
							String week = attr[5];
							if ( week.equals(title[m+8]) ) {
								writer.write(",1,-1");
							}
							else {
								writer.write(",-1,1");
							}
							
							//fill date: m+10 - m+16
							for ( int j = m+10; j <= m+16; j++ ) {
								String date = attr[4];
								if ( date.equals(title[j]) ) {
									writer.write(",1");
								}
								else {
									writer.write(",-1");
								}
							}
							
							//fill destinations: m+17 - m+n+16
							for ( int j = m+17; j<= m+n+16; j++ ) {
								String des = attr[2].replace(" ", "");
								if ( des.equals(title[j]) ) {
									writer.write(",1");
								}
								else {
									writer.write(",-1");
								}
							}
							
							writer.write("\n");
						}
						
					
						writer.close();
						writer2.close();
					}
					catch (Exception e) {
						e.printStackTrace(System.err);
					}
					
					previousVin = vin;
					data = null;
					data = new ArrayList<String[]>();
					data.add(fields);
					
					origins = null;
					origins = new HashMap<String, String>();
					if ( fields[1].length() > 3 ) {
						origins.put(fields[1], "");
					}
					destinations = null;
					destinations = new HashMap<String, String>();
					if ( fields[2].length() > 3 ) {
						destinations.put(fields[2], "");
					}
				}
			}
			
			reader.close();
		}
		catch (Exception e) {
			e.printStackTrace(System.err);
		}
	}
	
	public void generateFeatures() {
		try {
			BufferedReader reader = new BufferedReader(new FileReader("/Users/mz/test/volvo/data/Volvo-Cleaned-data-ordered.txt"));
			FileWriter writer = new FileWriter("/Users/mz/test/volvo/data/features.txt");
			String line = null;
			while( (line = reader.readLine()) != null ) {
				line = line.trim();
				String features = processLine(line);
				writer.write(features + "\n");
			}
			
			reader.close();
			writer.close();
		}
		catch (Exception e) {
			e.printStackTrace(System.err);
		}
	}
	
	public String processLine(String rawData) {
    	VolvoTrip trip = VolvoTrip.parse(rawData);
    	if ( trip == null ) {
    		return null;
    	}
    	
    	VolvoFeature feature = new VolvoFeature(trip);
    	fillParkingTime(feature);
    	
    	return feature.asRFriendlyFeatureVector(); //.asClassificationFeatureVector();
	}
	
	private void fillParkingTime(VolvoFeature feature) {
		if ( feature == null || feature.getDuration() != null ) {
			return;
		}
		
		Long lastEndTime = endTimeOfLastTrip.get(feature.getVehicleId());
		if ( lastEndTime == null ) {
			feature.setDuration("NA");
		}
		else {
			feature.setDuration("" + (convertParkingTime(feature.getStartTime() - lastEndTime)));
		}
		
		//update the cache
		endTimeOfLastTrip.put(feature.getVehicleId(), feature.getEndTime());
	}

	//public static int convertParkingTime(long parking) {
		//at first convert milliseconds to minutes
	//	return (int) (parking / 1000 / 60);
	//}
	
	public static int convertParkingTime(long parking) {
		//at first convert milliseconds to minutes
		parking = parking / 1000 / 60;
		
		//make minimum parking time as 15 min
		if ( parking < 15 ) {
			parking = 15L;
		}
		
		int check = 15;
		int endCheck = 960;
		while ( check <= endCheck ) {
			if ( parking <= check ) {
				if ( check <= 15 ) {
					return check;
				}
				else {
					int preCheck = check / 2;
					int meanCheck = (preCheck + check) / 2;
					if ( parking < meanCheck ) {
						return preCheck;
					}
					else {
						return check;
					}
				}
			}
			 
			check = check * 2;
		}
		
		int MIN_IN_A_DAY = 60 * 24; 
		return (((int)parking/MIN_IN_A_DAY + 1) * MIN_IN_A_DAY);
	}
	
	public static int getTime(String time) {
		for ( int i = 0; i < TIME_IN_A_DAY.length; i++ ) {
			if ( TIME_IN_A_DAY[i].equals(time) ) {
				return i;
			}
		}
		
		return TIME_IN_A_DAY.length;
	}
	
	public static double distFrom(double lat1, double lng1, double lat2, double lng2) {
	    double earthRadius = 6371000; //meters
	    double dLat = Math.toRadians(lat2-lat1);
	    double dLng = Math.toRadians(lng2-lng1);
	    double a = Math.sin(dLat/2) * Math.sin(dLat/2) +
	               Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) *
	               Math.sin(dLng/2) * Math.sin(dLng/2);
	    double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
	    double dist = earthRadius * c;

	    return dist;
 }
}
