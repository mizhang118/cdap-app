/**
 * @author Ming Zhang
 * created at 3/24/2017
 * 
 * The class trains Destination and duration model for volvo trips
 */

package com.ericsson.pm.analytics;

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
	private String rScriptPath;
	private String dataFilePath;
	private String trainingType;

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
	
	public List<AprioriRule> processResult(String rPath, String file) {
		Process rExecutorforDurationAndDestination = null;
		try {
			//rExecutor = Runtime.getRuntime().exec("Rscript " + rScriptPath + " " + dataFilePath+" "+vin+" "+storage.getPlotsSink());
				//rExecutorforDurationAndDestination = Runtime.getRuntime().exec("Rscript "+rScriptPath +" "+"/home/c26/c26-analytics/src/main/java/com/ericsson/c26/analytics/recommendations/R/duration1.txt"); 
			rExecutorforDurationAndDestination = Runtime.getRuntime().exec("Rscript "+ rPath + "/" + file + " " + dataFilePath); 
				log.trace("Executing r script " + rScriptPath + " for predicting {} with file " + dataFilePath, file);
			
		} catch (IOException e) {
			log.error("IOException while executing R command process");
			e.printStackTrace();
			return null;
		}
		
		BufferedReader destinationAndDurationReader;
		String resultLine,destinationAndDurationResult = null;
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
		//Differentiating post-processing of results for media, trip & Destination recommendations
		//Different post-processing for destination and duration
		//if(!trainingType.equals(ControlMessageModelTrainingTypeCommand.TRIP)&&!trainingType.equals(ControlMessageModelTrainingTypeCommand.MEDIA))
		List<AprioriRule> destinationAndDurationRules = null;
		//if ( trainingType.equals(ControlMessageModelTrainingTypeCommand.DESTINATION) || 
		//	 trainingType.equals(ControlMessageModelTrainingTypeCommand.DURATION) )
		//{
			AprioriResult result = new AprioriResult(destinationAndDurationOutput);
			destinationAndDurationRules = filterDestinationAndDurationRules(result.getRules());
			int intputTrips = countLinesInFile(dataFilePath);
			log.trace("Vin: " + vin + ", R " + file + ", data: " + dataFilePath + ", input=output: " + countLinesInFile(dataFilePath) + "="  + destinationAndDurationRules.size());	
			//log.trace("First rule from thread is "+destinationAndDurationRules.get(0).toString());
			//storage.saveDestinationAndDurationRules(vin, destinationAndDurationRules);
		//}
		
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

}

