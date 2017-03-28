/**
 * @author Ming Zhang
 * created at 3/24/2017
 * 
 * The class trains Destination and duration model for volvo trips
 */

package com.ericsson.pm.c26.analytics;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DoTrainRecommendationModelForVolvo {
	private static final String PREDICT_DESTINATION = "predictStops.R";
	private static final String PREDICT_DURATION = "predictDuration.R";
	private static final String PREDICT_DESTINATION_AND_DURATION = "predictDestinationAndDuration.R";
	
	private static final Logger log = LoggerFactory.getLogger(DoTrainRecommendationModelForVolvo.class);
	private String vin;
	private String r = "/Library/Frameworks/R.framework/Resources/bin/Rscript";
	private String rScriptPath = "/tmp/CDAP/predictionDestinationAndDuration.R";
	private String dataFilePath = "/tmp/CDAP/data.csv";
	private String trainingType = "destinationAndDuration";
	
	public DoTrainRecommendationModelForVolvo(String vin) {
		this.vin = vin;
	}

	public DoTrainRecommendationModelForVolvo(String vin, String rScriptPath, String dataFilePath, String type) {
		this.vin = vin;
		this.rScriptPath = rScriptPath;
		this.dataFilePath = dataFilePath;
		this.trainingType = type;
	}

	private List<AprioriRule> filterDestinationAndDurationRules(List<AprioriRule> rules) {
		List<AprioriRule> result = new ArrayList<AprioriRule>();
		for (AprioriRule rule: rules) {
//filters to be implemented later
			result.add(rule);
		}
		return result;
	}
	
	public List<AprioriRule> processResult() {
		Process rExecutorforDurationAndDestination = null;
		try {
			rExecutorforDurationAndDestination = Runtime.getRuntime().exec(r + " " + rScriptPath + " " + dataFilePath); 
				log.trace("Executing r script " + rScriptPath + " for predicting {} with file " + dataFilePath);
			
		} catch (IOException e) {
			log.error("IOException while executing R command process");
			e.printStackTrace();
			return null;
		}
		
		BufferedReader destinationAndDurationReader;
		String destinationAndDurationResult = null;
		List<String> destinationAndDurationOutput = new ArrayList<String>();		
		destinationAndDurationReader = new BufferedReader(new InputStreamReader(rExecutorforDurationAndDestination.getInputStream()));
		try {
			while ((destinationAndDurationResult = destinationAndDurationReader.readLine()) != null) {
				//log.info(destinationAndDurationResult);
				destinationAndDurationOutput.add(destinationAndDurationResult); 
			}

		} catch (IOException e) {
			log.error("IOException while iterating through R command process output");
			e.printStackTrace();
		}

		List<AprioriRule> destinationAndDurationRules = null;
		AprioriResult result = new AprioriResult(destinationAndDurationOutput);
		destinationAndDurationRules = filterDestinationAndDurationRules(result.getRules());
		int intputTrips = countLinesInFile(dataFilePath);
		log.trace("Vin: " + vin + ", R " + rScriptPath + ", data: " + dataFilePath + ", input=output: " + intputTrips + "="  + destinationAndDurationRules.size());
		
		return destinationAndDurationRules;
	}
	
	private int countLinesInFile(String file) {
		int count = 0;
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(file));
			String line = null;
			while ( (line = reader.readLine()) != null ) {
				line = line.trim();
				if ( line.length() > 0 ) {
					count++;
				}
			}
		}
		catch (IOException e) {
		}
		finally {
			try { if ( reader != null ) reader.close(); } catch (IOException ee) {}
		}
		
		return count;
	}
	
	public static void main(String[] args) {
		DoTrainRecommendationModelForVolvo trainer = new DoTrainRecommendationModelForVolvo("12345678");
		List<AprioriRule> rules = trainer.processResult();
		System.out.println(rules.size());
		for ( AprioriRule rule : rules ) {
			System.out.println(rule.toJson().toString());
		}
	}

}

