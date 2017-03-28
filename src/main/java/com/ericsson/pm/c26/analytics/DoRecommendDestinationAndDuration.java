/**
 * @author Ming Zhang
 * created at 3/27/2017
 * 
 * The class is ported from c26analytics project without changes
 */

package com.ericsson.pm.c26.analytics;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.pm.c26.analytics.ModelQueryResult;
import com.ericsson.pm.c26.common.C26AnalyticsConstants;

public class DoRecommendDestinationAndDuration {

	private static final Logger log = LoggerFactory.getLogger(DoRecommendDestinationAndDuration.class);
	private String vin;
	private String origin;
	private String duration;
	private String time;
	private String dayOfWeek;
	private String dayType;
	private String sort;
	private String network;
	
	private int durationNum = -1;
	private static int DURATION_RANGE = 5; //+ or - 5
	
	private String result = "";
	JSONArray recommendationArray;
	JSONObject recommendation;
	
	private ModelQueryResult modelQueryResult = new ModelQueryResult();

	public DoRecommendDestinationAndDuration(String vin, String origin) {
		log.debug("Creating DoRecommendDestinationAndDuration thread for " + vin);
		this.vin = vin;
		this.origin = origin;
		
		if ( vin == null ) {
			log.error("VIN was not defined in DoRecommendDestinationAndDuration");
		}
	}
	
	public DoRecommendDestinationAndDuration(Map<String, String> params) {
		log.debug("Creating DoRecommendDestinationAndDuration thread for " + vin);
		Set<String> keyset = params.keySet();
		Iterator<String> keyiter = keyset.iterator();
		while ( keyiter.hasNext() ) {
			String key = keyiter.next();
			String value = params.get(key);
			if ( value != null ) {
				value = value.toLowerCase();
			}
			switch(key) {
				case C26AnalyticsConstants.VIN: this.vin = value; break;
				case C26AnalyticsConstants.ORIGIN: this.origin = value; break;
				case C26AnalyticsConstants.DURATION: this.duration = value; try { durationNum = Integer.parseInt(duration); } catch (Exception e) {} break;
				case C26AnalyticsConstants.TIME_OF_DAY: this.time = value; break;
				case C26AnalyticsConstants.DAY_OF_WEEK: this.dayOfWeek = value; break;
				case C26AnalyticsConstants.DAY_TYPE: this.dayType = value; break;
				case C26AnalyticsConstants.SORTBY: this.sort = value; break;
				case C26AnalyticsConstants.NETWORK: this.network = value; break;
				default: log.error("Unknown paramter: " + key);
			}
		}
		
		if ( vin == null ) {
			log.error("VIN was not defined in DoRecommendDestinationAndDuration");
		}
		
		log.debug(String.format("Params from Map constructor: vin=%s, origin=%s, duration=%s, time=%s, sort=%s, duraitonNum=%d", this.vin, this.origin, this.duration, this.time, this.sort, this.durationNum));
	}
	
	public String getRuleString() {
		List<AprioriRule> rules = null; //storage.getDestinationAndDurationRules(vin);
		recommendationArray = new JSONArray();
		log.trace("Fetched " + rules.size());
		
		LinkedList<AprioriRule> list = new LinkedList<AprioriRule>();
		for(AprioriRule rule : rules) {
			log.info("Elements in rule: {}", rule.getElements());
			
			try {
				if ( origin != null ) {
					String originInRule = rule.getElements().get(C26AnalyticsConstants.ORIGIN);
					if ( originInRule != null ) {
						originInRule = originInRule.toLowerCase();
					}
					else {
						continue;
					}
					if ( originInRule.contains(origin) ) {
						;
					}
					else {
						continue;
					}
				}
				if ( durationNum > -1 ) {
					Long durationInRule = fromStringToNumber(rule.getElements().get(C26AnalyticsConstants.DURATION));
					if ( durationInRule == null ) {
						continue;
					}
					int diff = Math.abs(durationNum - (int) ((long) durationInRule));
					if( diff <= DURATION_RANGE ) {
						;
					}
					else {
						continue;
					}				
				}
				if ( time != null ) {
					String timeInRule = rule.getElements().get(C26AnalyticsConstants.TIME_OF_DAY);
					if ( timeInRule != null ) {
						timeInRule = timeInRule.toLowerCase();
					}
					else {
						continue;
					}
					if ( timeInRule.contains(time) ) {
						;
					}
					else {
						continue;
					}
				}
				if ( dayOfWeek != null ) {
					String dayOfWeekInRule = rule.getElements().get(C26AnalyticsConstants.DAY_OF_WEEK);
					if ( dayOfWeekInRule != null ) {
						dayOfWeekInRule = dayOfWeekInRule.toLowerCase();
					}
					else {
						continue;
					}
					// It could match all string or first characters
					if ( dayOfWeekInRule.indexOf(dayOfWeek) == 0 ) {
						;
					}
					else {
						continue;
					}
				}
				if ( dayType != null ) {
					String dayTypeInRule = rule.getElements().get(C26AnalyticsConstants.DAY_TYPE);
					if ( dayTypeInRule != null ) {
						dayTypeInRule = dayTypeInRule.toLowerCase();
					}
					else {
						continue;
					}
					if ( dayTypeInRule.equals(dayType) ) {
						;
					}
					else {
						continue;
					}
				}
			
				//Now, origin, duration and time are all null or they are all matched when they are not null
				//result += rule.toString()+"\n";
				insertRule(list, rule, sort);
			}
			catch (Exception e) {
				log.error("Fail to filter query parameters", e);
			}
			
			log.info("Elements rule String: " + result);

		}
		
		/**
		 * Filter out duplicated rules using origin, destination and duration
		 */
		
		list = removeDuplicateRules(list);
		
		/**
		 * Cache the results of KPI web service call.
		 */
		Map<String, JSONArray> kpiCache = new HashMap<String, JSONArray>(100);
		for ( AprioriRule rule : list ) {
			//result += rule.toString()+"\n";
			recommendation = rule.toJson();
			//recommendationArray.put(recommendation);
			JSONArray kpi = null;
			if ( network != null && network.equals("no") ) {
				kpi = new JSONArray();
			}
			else { 
				String destinationAddress = rule.getElements().get(C26AnalyticsConstants.DESTINATION);
				kpi = createNetworkKpi(destinationAddress);
				//kpiCache.put(destinationAddress, kpi);
			}
			modelQueryResult.add(recommendation, kpi);
		}
		
		//return result;
		//recommendationArray.toString();
		return modelQueryResult.getModelQueryResult().toString();
	}
	
	public void insertRule(LinkedList<AprioriRule> list, AprioriRule value, String sortBy) {
		int size = list.size();
        if ( size == 0 ) {
            list.add(value);
        } else if ( compareArioriRule(list.get(0), value, sortBy) <= 0 ) {
            list.add(0, value);
        } else if ( compareArioriRule(list.get(size - 1), value, sortBy) >= 0 ) {
            list.add(size, value);
        } else {
            int i = 0;
            while ( i < size && compareArioriRule(list.get(i), value, sortBy) > 0 ) {
                i++;
            }
            list.add(i, value);
        }
	}
	
	public int compareArioriRule(AprioriRule r1, AprioriRule r2, String sortBy) {
		//by default sorted by confidence
		double sort1 = r1.getSupport() * r1.getLift() * r1.getConfidence();
		double sort2 = r2.getSupport() * r2.getLift() * r2.getConfidence();
		
		if ( sortBy != null ) {
			if ( sortBy.equals(C26AnalyticsConstants.SUPPORT) ) {
				sort1 = r1.getSupport();
				sort2 = r2.getSupport();
			}
			else if ( sortBy.equals(C26AnalyticsConstants.LIFT) ) {
				sort1 = r1.getLift();
				sort2 = r2.getLift();
			}
			else if ( sortBy.equals(C26AnalyticsConstants.CONFIDENCE) ) {
				sort1 = r1.getConfidence();
				sort2 = r2.getConfidence();
			}
		}
		
		double diff = sort1 - sort2;
		if ( diff == 0 ) {
			return 0;
		}
		else if ( diff < 0 ) {
			return -1;
		}
		else {
			return 1;
		}
	}
	
	public static JSONArray createNetworkKpi(String destination, Double longitude, Double latitude) {
		JSONArray kpi = new JSONArray();

		if ( destination == null ) {
			log.error("Destination is not available.");
			return kpi;
		}
		
		/**
		 * Look up cache if network KPI is in cache, otherwise
		 * query it from two-step API calls (google and network_kpi).
		 * The cache should be removed after postgreSQL GIS becomes faster.
		 */
/*
		ApiCache cache = GoogleGeocoding.getApiCache();
		if ( cache != null ) {
			log.trace("Try to get network KPI from cache");
			//add ___ to avoid key collision
			String json = cache.get(destination + "___"); 
			if ( json != null ) {
				try {
					kpi = new JSONArray(json);
					log.trace("Got network KPI from cache");
					return kpi;
				}
				catch (JSONException e) {
					log.error("Wrong JSON format {}", json, e);
				}
			}
		}
		
		log.trace("Did not get network API from cache so that try to get network KPI from two-step API calls (goolge and network_kpi)");		
		
		if ( longitude == null || latitude == null ) {
			Double[] lonLat = GoogleGeocoding.getCoordinatesByAddress(destination);
			if ( lonLat == null || lonLat.length != 2 || lonLat[0] == null || lonLat[1] == null ) {
				return kpi;
			}
			longitude = lonLat[0];
			latitude = lonLat[1];
		}
		
		//Query URL looks like: http://52.32.252.183/signal_strength?lng=-71.26084978&lat=42.39568795
		if ( Configuration.networkUrl != null ) {
			String url = Configuration.networkUrl + "?lng=" + longitude + "&lat=" + latitude + "&radius=100";
			log.trace(url);
			RestClient restClient = new RestClient(url);
			JSONObject response = restClient.getJSONObject();
			if ( response != null ) {
				log.trace(response.toString());
				JSONObject result = response.getJSONObject("results");
				if ( result != null ) {
					@SuppressWarnings("rawtypes")
					Iterator keyIter = result.keys();
					while( keyIter.hasNext() ) {
						Object key = keyIter.next();
						kpi.put(result.get((String)key));
					}
					
					//put network KPI data into cache. This will be removed after network API becomes faster
					if ( cache != null ) {
						log.trace("put network KPI into cache: {}", destination);
						// append ___ into key in order to avoid key collision
						cache.put(destination + "___", kpi.toString());
					}
				}
			}
			else {
				log.info("Get null response for query {}", url);
			}
		}
		else {
			log.error("Configuration.networkUrl was not initialized in C26AnalyticsApplication");
		}
*/		
		return kpi;
	}
	
	public static JSONArray createNetworkKpi(String destination) {
		return createNetworkKpi(destination, null, null);
	}
	
	private LinkedList<AprioriRule> removeDuplicateRules (LinkedList<AprioriRule> list) {
		LinkedList<AprioriRule> filtered = new LinkedList<AprioriRule>();
		if ( list == null ) {
			return filtered;
		}
		
		Map<String, String> check = new HashMap<String, String>(800);
		for (AprioriRule rule : list) {
			String uniqueKey = rule.getElements().get(C26AnalyticsConstants.ORIGIN) + "_" + rule.getElements().get(C26AnalyticsConstants.DESTINATION) + "_" + rule.getElements().get(C26AnalyticsConstants.DURATION);
			if ( check.get(uniqueKey) == null ) {
				check.put(uniqueKey, "");
				filtered.add(rule);
			}
		}
		
		return filtered;
	}
	
    public static Long fromStringToNumber(String str) {
    	Long ret = null;
    	if ( str == null ) {
    		return null;
    	}
    	
    	int idx = str.indexOf("MIN");
    	if ( idx < 0 ) {
    		return null;
    	}
    	
    	String numString = str.substring(0, idx);
    	try { ret = new Long(numString); } catch (Exception e) {}
    	
    	return ret;
    }
}
