/**
 * @author Ming Zhang
 * created at 3/27/2017
 * 
 * The class is ported from c26analytics project without changes
 */

package com.ericsson.pm.c26.analytics;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.pm.c26.entities.VolvoFeature;
import com.ericsson.pm.c26.entities.VolvoTrip;

public class RecommendationServerForVolvo {
	private static final Logger LOG = LoggerFactory.getLogger(RecommendationServerForVolvo.class);
	private static final String tripsTemporaryDirectory = "/tmp";
	private String rScriptPath = "/tmp/CDAP/predictionDestinationAndDuration.R";

	public RecommendationServerForVolvo() {
	}

	public List<AprioriRule> trainModel(String vin, String type, List<VolvoFeature> f) {
		LOG.info("Scheduling model training for vin " + vin);
		List<VolvoFeature> features = new ArrayList<VolvoFeature>();
		if ( f != null ) {
			features = f;
		}
		List<AprioriRule> rules = new ArrayList<AprioriRule>();
		LOG.info("Obtained {} trips for vin {} to train model", features.size(), vin);
		if (features.size() > 0) {
			String dataFilePath = dumpTripsRVectorToFile(features);
			
			DoTrainRecommendationModelForVolvo trainStop = new DoTrainRecommendationModelForVolvo(vin, rScriptPath, dataFilePath,type);
			rules = trainStop.processResult();
		}
		
		return rules;
	}

	
	public void recommendDestinationAndDuration(String vin, String origin) {
		DoRecommendDestinationAndDuration recommendWorker= new DoRecommendDestinationAndDuration(vin, origin);
		String ruleString = recommendWorker.getRuleString();
	}
	
	public String recommendDestinationAndDuration(Map<String, String> params) {
		DoRecommendDestinationAndDuration recommendThread= new DoRecommendDestinationAndDuration(params);
		return recommendThread.getRuleString();
	}

	public void trainAll() {
		List<String> vins = null; //storage.getTripsUsers();
		LOG.info("Triggering training for all " + vins.size() + " vins" );
		for (String vin: vins) trainModel(vin,"durationAndDestination", null);
	}

	private String dumpTripsRVectorToFile(List<VolvoFeature> features) {
		LOG.trace("No of trips queried from elasticsearch {}", features.size());
		String content = VolvoFeature.getRFriendlyFeatureVectorHeaders();
		for (VolvoFeature feature: features) {
			content += "\n" + feature.asRFriendlyFeatureVector();
		}
		String fileUuid = UUID.randomUUID().toString();
		String fileFullPath = tripsTemporaryDirectory + "/" + fileUuid + ".csv";
		try {
			FileUtils.writeStringToFile(new File(fileFullPath), content);
		} catch (IOException e) {
			LOG.error("IOException thrown while dumping trip data to temporary file.");
			e.printStackTrace();
		}
		return fileFullPath;
	}
	
	public static void main(String[] args) {
		String data = "/Users/mz/test/volvo/data/Volvo-Cleaned-data-ordered-vin-14867.txt";
		List<VolvoFeature> features = new ArrayList<VolvoFeature>();
		try {
			BufferedReader reader = new BufferedReader(new FileReader(data));
			String line = null;
			while ( (line = reader.readLine()) != null ) {
				System.out.println(line);
				
				VolvoTrip trip = VolvoTrip.parse(line);
				VolvoFeature feature = new VolvoFeature(trip);
				features.add(feature);
			}
		}
		catch (Exception e) {
			e.printStackTrace(System.err);
		}
		
		RecommendationServerForVolvo formatter = new RecommendationServerForVolvo();
		List<AprioriRule> rules = formatter.trainModel("1234", "destinationAndDuration", features);
		System.out.println(rules.size());
		for( AprioriRule rule : rules ) {
			System.out.println(rule.toJson().toString());
		}
		
	}

}