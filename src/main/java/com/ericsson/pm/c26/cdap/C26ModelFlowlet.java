/**
 * @author Ming Zhang
 * created at 3/17/2017
 * 
 * ModelFlowlet train features into model and save models into modelStore
 */

package com.ericsson.pm.c26.cdap;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.pm.c26.analytics.AprioriRule;
import com.ericsson.pm.c26.analytics.RecommendationServerForVolvo;
import com.ericsson.pm.c26.entities.VolvoFeature;
import com.ericsson.pm.c26.utilities.IOUtil;
import com.google.gson.Gson;

import co.cask.cdap.api.annotation.HashPartition;
import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.annotation.Tick;
import co.cask.cdap.api.annotation.UseDataSet;

public class C26ModelFlowlet extends GenericFlowlet {
	private static final Logger LOG = LoggerFactory.getLogger(C26ModelFlowlet.class);
	
	// dataset for query features
	@UseDataSet("c26FeatureStore")
	private C26TripDataset featureStore;
	
	// dataset for storing models
	@UseDataSet("c26ModelStore")
	private C26TripDataset modelStore;
	
	private Map<String, Long> vinForModelling = new ConcurrentHashMap<String, Long>(2048);

	@HashPartition("vin")
	@ProcessInput
	public void processModel(String vin) {
		//LOG.info("Start C26ModelFlowlet to process vin {}", vin);
		
		// put vin into map for future training
		if ( vin != null ) {
			Long timestamp = System.currentTimeMillis();
			vinForModelling.put(vin, timestamp);
		}
	}
	
	/**
	 * do model training here
	 * This agent wake up every 10 seconds and check vinForModelling map
	 * If any vin is put inside the map, and the vin did not updated in 5 seconds, 
	 * remove the vin from map and start to training. Otherwise skip.
	 * This way can skip training in the process of batch load. 
	 * But it trains every time in single trip load with up to 15 seconds delay
	 * 
	 * @throws InterruptedException
	 */
	@Tick(delay = 10000L, unit = TimeUnit.MILLISECONDS)
	public void trainModel() throws InterruptedException {
/*
		LOG.info("Run a command from CDAP container");
        try {
        	LOG.info("Before establish Runtime");
            Runtime rt = Runtime.getRuntime();
            LOG.info("After establish Runtime");
            Process pr = rt.exec("ls -l /Users/mz/test/volvo/data");
            LOG.info("After exec command");
            BufferedReader input = new BufferedReader(new InputStreamReader(pr.getInputStream()));
            LOG.info("Open output stream");
            String line=null;
            while((line=input.readLine()) != null) {
                LOG.info(line);
            }

            int exitVal = pr.waitFor();
            LOG.info("Exited with error code "+exitVal);

        } catch(Exception e) {
            LOG.error("Fail to run command", e);
        }
        
		LOG.info("Do several testing on file system...");
		File file = new File("/tmp/de5127e9-ca29-45dc-955d-ae1e20b88dd5.csv");
		if ( file.exists() ) {
			LOG.info("Test file was found!!!!!!!!!!");
		}
		else {
			LOG.info("Failed to find test file?????????");
		}
		//IOUtil.saveFile("/tmp/saveFile", "use apache common IO");
		IOUtil.saveFile("/Users/mz/test/volvo/data/saveFile", "use apache common IO");
		//IOUtil.saveFile2("/tmp/saveFile2", "use FileWriter");
		IOUtil.saveFile2("/Users/mz/test/volvo/data/saveFile2", "use FileWriter");
		LOG.info("Done testing on file system...");
*/		
		LOG.info("Start model training, check any vin is available for trainning...");
		Set<String> vinset = vinForModelling.keySet();
		for (String vin : vinset) {
			LOG.info("Check vin {} ...", vin);
		    Long timestamp = vinForModelling.get(vin);
		    long curTimestamp = System.currentTimeMillis();
		    //if the vin does not updated in last 5 seconds, do training
		    //otherwise, skip
		    if ( (curTimestamp - timestamp) > 5000 ) {
		    	//At first delete the vin from vinForModelling. Please delete vin before training!
		    	LOG.info("Start to train vin {}", vin);
		    	vinForModelling.remove(vin);
		    	
		    	//Then train model of the vin
		    	//first step: get all features from dataset (c26FeatureStore), and put features into a List<C26Feature>
		    	List<VolvoFeature> features = new ArrayList<VolvoFeature>();
		    	Map<String, String> data = featureStore.getData(vin);
		    	for (Map.Entry<String, String> pair : data.entrySet()) {
		    		String key = pair.getKey();
		    	    String value = pair.getValue();

		    	    VolvoFeature feature = new VolvoFeature(value);
		    	    if ( feature.getId() != null && feature.getId().equals(key) ) {
		    	    	features.add(feature);
		    	    	modelStore.addData(vin, feature.getId(), feature.asRFriendlyFeatureVector());
		    	    }
		    	    else {
		    	    	LOG.error("The feature object (id {}) was not created correctly. Feature object id {}", key, feature.getId());
		    	    }
		    	    
		    	}
		    	
		    	//second step: print feature data onto a file and run Rscript to train model and parse results
		    	LOG.info("{} features are retrieved. Create RecommandationServerForVolvo to do training...", features.size());
				RecommendationServerForVolvo formatter = new RecommendationServerForVolvo();
				List<AprioriRule> rules = formatter.trainModel(vin, "destinationAndDuration", features);
				LOG.info("Train vin {} and get {} rules.", vin, rules.size());		    	
		    	
		    	//third step: save models into dataset (c26ModelStore)
				int count = 1;
				Gson gson = new Gson();
				for( AprioriRule rule : rules ) {
					modelStore.addData(vin,"" + count, gson.toJson(rule));
					count++;
				}
		    	
		    }
		    else {
		    	LOG.info("Skip vin {}. This vin may be trainned next time", vin);
		    }
		}
		
	}
}