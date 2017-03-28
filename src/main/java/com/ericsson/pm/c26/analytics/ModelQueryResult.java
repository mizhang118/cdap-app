package com.ericsson.pm.c26.analytics;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ModelQueryResult integrates model query results (refer to DoRecommendDestinationAndDuration.java)
 * and network query results (refer to NetworkQueryResult.java)
 * 
 * The issue is that NetworkQueryResult.java uses GSON to do the conversion between object and JSON,
 * while DoRecommendDestinationAndDuration.java uses JACKSON (JSONObject and JSONArray) to do the conversion.
 * This integration class tries to use JACKSON to put both together.
 * 
 * @author mz
 *
 */
public class ModelQueryResult {
	private static final Logger LOG = LoggerFactory.getLogger(ModelQueryResult.class);
	
	public static final String RECOMMENDATIONS = "recommendations";
	public static final String DESTINATION_AND_DURATION = "destinationAndDuration";
	public static final String NETWORK_KPI = "network_kpi";
	
	private JSONObject modelQueryResult = new JSONObject();
	private JSONArray recommendations = new JSONArray();
	
	public ModelQueryResult() {
		try {
		modelQueryResult.put(RECOMMENDATIONS, recommendations);
		}
		catch (JSONException e) {
			LOG.error("Failed to build model result JSON", e);
		}
	}
	
	public JSONObject getModelQueryResult() {
		return modelQueryResult;
	}
	
	public void add(JSONObject rule, JSONArray network_kpi) {
		JSONObject item = new JSONObject();
		try {
			item.put(DESTINATION_AND_DURATION, rule);
			item.put(NETWORK_KPI, network_kpi);
		}
		catch(JSONException e) {
			LOG.error("Failed to put rules and/or network_api data into JSON", e);
		}
		
		recommendations.put(item);
	}

}

