/**
 * @author Ming Zhang
 * created at 3/16/2017
 * C26 CDAP application entry point
 */

package com.ericsson.pm.c26.cdap;

import com.ericsson.pm.c26.spark.AssociationRule;

import co.cask.cdap.api.Config;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.schedule.Schedules;
import co.cask.cdap.api.spark.AbstractSpark;

public class C26AnalyticsApp extends AbstractApplication<Config> {
	public static final String APP_NAME = "c26Analytics";
	public static final String STREAM_NAME = "c26Stream";
	public static final String DATASET_TRIP_STORE = "c26TripStore";
	public static final String DATASET_FEATURE_STORE = "c26FeatureStore";
	public static final String DATASET_TRAIN_STORE = "c26TrainStore";
	public static final String DATASET_MODEL_STORE = "c26ModelStore";
	public static final String WORKFLOW_MODEL_TRAIN = "c26ModelTrainWorkflow";
	public static final String SERVICE_TRIP = "c26TripService";
	public static final String SERVICE_FEATURE = "c26FeatureService";
	public static final String SERVICE_MODEL = "c26ModelService";
	
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
	    // Custom dataset c26TrainStore to store vin for training
	    createDataset(DATASET_TRAIN_STORE, C26TripDataset.class);
	    // Custom dataset c26FeatureStore to store ML features extracted from trip
	    createDataset(DATASET_MODEL_STORE, C26TripDataset.class);
	    
	    // Add the c26 flow
	    addFlow(new C26Flow());

	    // Add the c26 trip services
	    addService(new C26TripService());

	    // Add the c26 feature services
	    addService(new C26FeatureService());

	    // Add the c26 model services
	    addService(new C26ModelService());
	    
	    //spark ML lib to train model
	    addSpark(new SparkAssoicationRule());
	    
	    //add workflow to run spark job
	    addWorkflow(new C26ModelTrainWorkflow());
	    
	    
	    //at first setup training interval
	    C26TripDataset.TRAIN_INTERVAL = 1000 * 60 * 60; // milliseconds of one hour
	    //schedule the workflow hourly that matches TRAIN_INTERVAL
	    scheduleWorkflow(Schedules.builder("Run Spark mllib hourly")
	    		                  .setMaxConcurrentRuns(1)
	                              .createTimeSchedule("0 * * * *"),
	                     WORKFLOW_MODEL_TRAIN);
	}
	
	  /**
	   * A Spark Program that uses KMeans algorithm.
	   */
	  public static final class SparkAssoicationRule extends AbstractSpark {

	    @Override
	    public void configure() {
	      setName("SparkAssociationRule");
	      setDescription("Spark Association Rule Program");
	      setMainClass(AssociationRule.class);
	    }
	  }
}
