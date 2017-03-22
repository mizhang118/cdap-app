/**
 * @author Ming Zhang
 * created at 3/17/2017
 * 
 * TripFlowlet parse input, build VolvoTrip objects and save the objects into dataset
 */

package com.ericsson.pm.c26.cdap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.pm.c26.entities.VolvoTrip;
import com.google.common.base.Charsets;

import co.cask.cdap.api.annotation.HashPartition;
import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.flow.flowlet.OutputEmitter;
import co.cask.cdap.api.flow.flowlet.StreamEvent;

public class C26TripFlowlet extends GenericFlowlet {
    private static final Logger LOG = LoggerFactory.getLogger(C26TripFlowlet.class);
    
	// UseDataSet annotation indicates the page-views Dataset is used in the Flowlet
	@UseDataSet("c26TripStore")
	private C26TripDataset tripStore;

    // Emitter for emitting a trip instance to the next Flowlet (c26FeatureFlowlet)
    private OutputEmitter<VolvoTrip> output;

    // Annotation indicates that this method can process incoming data
    @ProcessInput
    public void processFromStream(StreamEvent event) {
    	// Get a log event in String format from a StreamEvent instance
    	String rawData = Charsets.UTF_8.decode(event.getBody()).toString();
    	VolvoTrip trip = VolvoTrip.parse(rawData);
    	if ( trip != null && trip.getVin() != null ) {
    		tripStore.addTrip(trip);
    		output.emit(trip, "vin", trip.getVin().hashCode());
    	}
    	else {
    		LOG.error("VolvoTrip is null or its vin is null");
    	}
    }
}
