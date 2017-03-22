/**
 * @author Ming Zhang
 * created at 3/16/2017
 * C26 CDAP application entry point
 */

package com.ericsson.pm.c26.cdap;

import co.cask.cdap.api.Config;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.data.stream.Stream;

public class C26AnalyticsApp extends AbstractApplication<Config> {
	public static final String APP_NAME = "c26Analytics";
	public static final String STREAM_NAME = "c26Stream";
	public static final String DATASET_TRIP_STORE = "c26TripStore";
	public static final String DATASET_FEATURE_STORE = "c26FeatureStore";
	public static final String DATASET_MODEL_STORE = "c26ModelStore";
	
	@Override
	public void configure() {
	    setName(APP_NAME);
	    setDescription("Predictive Mobility C26 Analytics CDAP Application");
	    // The c26Stream will receive the trip raw data from self-driving car
	    addStream(new Stream(STREAM_NAME));
	    // Custom dataset c26TripStore to store trip info as JSON
	    createDataset(DATASET_TRIP_STORE, C26TripDataset.class);
	    // Custom dataset c26FeatureStore to store ML features extracted from trip
	    createDataset(DATASET_FEATURE_STORE, C26TripDataset.class);
	    // Custom dataset c26FeatureStore to store ML features extracted from trip
	    createDataset(DATASET_MODEL_STORE, C26TripDataset.class);
	    
	    // Add the c26 flow
	    addFlow(new C26Flow());

	    // Add the c26 services
	    //addService(new WiseService());
	}
}
