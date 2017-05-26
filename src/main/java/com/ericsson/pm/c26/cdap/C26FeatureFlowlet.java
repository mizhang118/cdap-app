/**
 * @author Ming Zhang
 * created at 3/17/2017
 * 
 * FeatureFlowlet extracts features and save features into dataset
 */

package com.ericsson.pm.c26.cdap;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.pm.c26.entities.VolvoFeature;
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
	
	@UseDataSet("c26TrainStore")
	private C26TripDataset trainStore;
	
    // Emitter for emitting a trip instance to the next Flowlet (c26ModelFlowlet)
    //private OutputEmitter<String> output;
    
    /**
     * Map<String, Long> caches the endtime of last trip of same vin.
     * Since the flowlet data transition is partitioned by vin, so that we the local cache is valid.
     * The cache depends on two pre-requirements: (1) trips have to be ordered by timestap. (2) data transition
     * has been partitioned by vin.
     */
    private Map<String, Long> endTimeOfLastTrip = new ConcurrentHashMap<String, Long>(2048);

	@HashPartition("vin")
	@ProcessInput
	public void processFeature(VolvoTrip trip) {
		// extract features and save them into c26FeatureStore
		VolvoFeature feature = new VolvoFeature(trip);
		fillParkingTime(feature);
		featureStore.addFeature(feature);
		
		trainStore.addData(feature.getVehicleId(), feature.getId(), feature.asSparkFriendlyFeatureVector());
		
		//output.emit(feature.getVehicleId(), "vin", feature.getVehicleId().hashCode());
	}
	
	private void fillParkingTime(VolvoFeature feature) {
		if ( feature == null || feature.getDuration() != null ) {
			return;
		}
		
		Long lastEndTime = endTimeOfLastTrip.get(feature.getVehicleId());
		if ( lastEndTime == null ) {
			feature.setDuration("15MIN");
		}
		else {
			feature.setDuration("" + (convertParkingTime(feature.getStartTime() - lastEndTime)) + "MIN");
		}
		
		//update the cache
		endTimeOfLastTrip.put(feature.getVehicleId(), feature.getEndTime());
	}
	
	/**
	 * 
	 * @param parking milliseconds
	 * @return
	 */
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
}