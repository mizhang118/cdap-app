/**
 * @author Ming Zhang
 * created at 3/27/2017
 * 
 * The class is ported from c26analytics project without changes
 */

package com.ericsson.pm.c26.analytics;

import java.util.HashMap;
import java.util.Map;

import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AprioriRule implements Comparable<AprioriRule> {
	private static final Logger LOG = LoggerFactory.getLogger(AprioriRule.class);
	private Map<String, String> elements;
	private double support;
	private double confidence;
	private double lift;

	public AprioriRule(String ruleText) {
		elements = new HashMap<String, String>();
		String[] fields = ruleText.split("=>");
		elements.putAll(loadRuleLeftSideFromString(fields[0]));
		elements.putAll(loadRuleRightSideFromString(fields[1]));
		String numbers = ruleText.split("}")[2].trim();
		String[] params = numbers.split("\\s+");
		support = Double.parseDouble(params[0]);
		confidence = Double.parseDouble(params[1]);
		lift = Double.parseDouble(params[2]);
	}

	private Map<String, String> loadRuleLeftSideFromString(String text) {
		Map<String, String> result = new HashMap<String, String>();
		if (text.contains("{}")) return result;
		String members = text.split("\\{")[1].split("}")[0];
		for (String member: members.split(",")) {
			String[] keyValue = member.split("=");
			String key = keyValue[0].trim();
			String value = keyValue[1].trim();
			result.put(key, value);
		}
		return result;
	}
	
	private Map<String, String> loadRuleRightSideFromString(String text) {
		Map<String, String> result = new HashMap<String, String>();
		if (text.contains("{}")) return result;
		String members = text.split("\\{")[1].split("}")[0];
		for (String member: members.split(",")) {
			String[] keyValue = member.split("=");
			String key = keyValue[0].trim();
			String value = keyValue[1].trim();
			result.put(key, value);
			result.put("criteria", key);
		}
		return result;
	}

	public String toString() {
		String ret = ruleSideToString(elements) + "\n";
		ret += "support: " + support + " confidence: " + confidence + " lift: " + lift;
		return ret;
	}
	
	public JSONObject toJson() {
		String pattern = ruleSideToString(elements);
		JSONObject recommendation = null;
		try {
			recommendation = new JSONObject(pattern.replace("=", ":"));
			recommendation.put("support", support);
			recommendation.put("confidence", confidence);
			recommendation.put("lift", lift);
		}
		catch (JSONException e) {
			LOG.error("Fail to build JSON of trip rule", e);
		}
		return recommendation;
	}

	@Override
	public int compareTo(AprioriRule anotherRule) {
		return Double.compare(support, anotherRule.getSupport());
	}

	public boolean elementsKeyValueMatch(String key, String value) {
		if (elements.containsKey(key)) if (elements.get(key).equals(value)) return true;
		return false;
	}

	private static String ruleSideToString(Map<String, String> ruleSide) {
		String ret = "{";
		String sep = "";
		for (String key: ruleSide.keySet()) {
			ret += sep + key + "=" + ruleSide.get(key);
			sep = ", ";
		}
		return ret + "}";
	}

	public Map<String, String> getElements() { return elements; }
	public double getSupport() { return support; }
	public double getConfidence() { return confidence; }
	public double getLift() { return lift; }

	public void setElements(Map<String, String> elements) { this.elements = elements; }
	public void setSupport(double support) { this.support = support; }
	public void setConfidence(double confidence) { this.confidence = confidence; }
	public void setLift(double lift) { this.lift = lift; }

}

