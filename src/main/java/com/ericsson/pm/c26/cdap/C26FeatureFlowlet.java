/**
 * @author Ming Zhang
 * created at 3/17/2017
 * 
 * FeatureFlowlet extracts features and save features into dataset
 */

package com.ericsson.pm.c26.cdap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.pm.c26.entities.VolvoTrip;

import co.cask.cdap.api.annotation.HashPartition;
import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.flow.flowlet.OutputEmitter;

public class C26FeatureFlowlet extends GenericFlowlet {
	private static final Logger LOG = LoggerFactory.getLogger(C26FeatureFlowlet.class);
	
	// UseDataSet annotation indicates the page-views Dataset is used in the Flowlet
	@UseDataSet("c26FeatureStore")
	private C26TripDataset featureStore;
	
    // Emitter for emitting a trip instance to the next Flowlet (c26ModelFlowlet)
    private OutputEmitter<VolvoTrip> output;

	@HashPartition("vin")
	@ProcessInput
	public void processFeature(VolvoTrip trip) {
		// extract features and save them into c26FeatureStore
		featureStore.addTrip(trip);
		
		output.emit(trip, "vin", trip.getVin().hashCode());
	}
}