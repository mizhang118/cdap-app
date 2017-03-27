/**
 * @author Ming Zhang
 * created at 3/27/2017
 * 
 * The class is ported from c26analytics project without changes
 */

package com.ericsson.pm.analytics;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.pm.c26.entities.VolvoFeature;

public class RecommendationServerForVolvo {
	private static final Logger LOG = LoggerFactory.getLogger(RecommendationServerForVolvo.class);
	private static final String tripsTemporaryDirectory = "/tmp";
	private String rScriptPath = "/home/c26/c26-analytics/src/main/java/com/ericsson/c26/analytics/recommendations/R";

	public RecommendationServerForVolvo() {
	}

	public int trainModel(String vin, String type) {
		LOG.info("Scheduling model training for vin " + vin);
		List<VolvoFeature> features = null; //storage.getVolvoTrips(vin);
		LOG.info("Obtained {} trips for vin {} to train model", features.size(), vin);
		if (features.size() > 0) {
			String dataFilePath = dumpTripsRVectorToFile(features);
			
			DoTrainRecommendationModelForVolvo trainStopThread = new DoTrainRecommendationModelForVolvo(vin, rScriptPath, dataFilePath,type);
		}
		return features.size();
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
		for (String vin: vins) trainModel(vin,"durationAndDestination");
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

}