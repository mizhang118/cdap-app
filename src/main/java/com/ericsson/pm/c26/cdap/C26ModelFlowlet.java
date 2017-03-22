/**
 * @author Ming Zhang
 * created at 3/17/2017
 * 
 * ModelFlowlet train features into model and save models into modelStore
 */

package com.ericsson.pm.c26.cdap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.pm.c26.entities.VolvoTrip;

import co.cask.cdap.api.annotation.HashPartition;
import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.annotation.UseDataSet;

public class C26ModelFlowlet extends GenericFlowlet {
	private static final Logger LOG = LoggerFactory.getLogger(C26ModelFlowlet.class);
	
	// UseDataSet annotation indicates the page-views Dataset is used in the Flowlet
	@UseDataSet("c26ModelStore")
	private C26TripDataset modelStore;

	@HashPartition("ip")
	@ProcessInput
	public void processModel(VolvoTrip trip) {
		// extract features and save them into c26FeatureStore
		modelStore.addTrip(trip);
	}
}