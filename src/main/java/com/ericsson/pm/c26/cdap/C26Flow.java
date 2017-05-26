/**
 * @author Ming Zhang
 * created at 3/17/2017
 * 
 * Flow controls the c26 Flowlets.
 */

package com.ericsson.pm.c26.cdap;

import co.cask.cdap.api.flow.AbstractFlow;

public class C26Flow extends AbstractFlow {
	public static final String FLOW_NAME = "c26Flow";
	public static final String FLOWLET_TRIP = "c26TripFlowlet";
	public static final String FLOWLET_FEATURE = "c26FeatureFlowlet";
	public static final String FLOWLET_MODEL = "c26ModelFlowlet";
	
	  @Override
	  public void configure() {
	    setName(FLOW_NAME);
	    setDescription("c26Flow parses trip raw data, extracts features and generates models");
	    addFlowlet(FLOWLET_TRIP, new C26TripFlowlet());
	    addFlowlet(FLOWLET_FEATURE, new C26FeatureFlowlet());
	    //addFlowlet(FLOWLET_MODEL, new C26ModelFlowlet());
	    connectStream(C26AnalyticsApp.STREAM_NAME, FLOWLET_TRIP);
	    connect(FLOWLET_TRIP, FLOWLET_FEATURE);
	    //connect(FLOWLET_FEATURE, FLOWLET_MODEL);
	  }
}
