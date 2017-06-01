package com.ericsson.pm.c26.entities;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VolvoModel extends Entity implements Writable, Comparable<VolvoModel>{
	/**
	 * 
	 */
	private static final long serialVersionUID = 4L;

	private static final Logger LOG = LoggerFactory.getLogger(VolvoModel.class);
	
	private Map<String, String> antecedents = new HashMap<String, String>();
	private Map<String, String> consequents = new HashMap<String, String>();
	private Double support;
	private Double confidence;
	private Double lift;
	
	public VolvoModel() {
		super();
	}
	
	public VolvoModel(String json) {
		super(json);
	}
	
	public void putAntecedent(String key, String value) {
		this.antecedents.put(key, value);
	}
	
	public String getAntecedent(String key) {
		return this.antecedents.get(key);
	}

	public void putConsequent(String key, String value) {
		this.consequents.put(key, value);
	}
	
	public String getConsequent(String key) {
		return this.consequents.get(key);
	}
	
	public Map<String, String> getAntecedents() {
		return antecedents;
	}

	public void setAntecedents(Map<String, String> antecedents) {
		this.antecedents = antecedents;
	}

	public Map<String, String> getConsequents() {
		return consequents;
	}

	public void setConsequents(Map<String, String> consequents) {
		this.consequents = consequents;
	}

	public Double getSupport() {
		return support;
	}

	public void setSupport(Double support) {
		this.support = support;
	}

	public Double getConfidence() {
		return confidence;
	}

	public void setConfidence(Double confidence) {
		this.confidence = confidence;
	}

	public Double getLift() {
		return lift;
	}

	public void setLift(Double lift) {
		this.lift = lift;
	}
	
	public static VolvoModel parse(String antec, String conseq, double confidence) {
		VolvoModel model = new VolvoModel();
		model.setConfidence(confidence);
		
		if ( antec != null ) {
			String[] ants = antec.split(",");
			for (int i = 0; i < ants.length; i++ ) {
				fillKeyValue(model.getAntecedents(), ants[i]);
			}
		}
		
		if ( conseq != null ) {
			String[] cons = conseq.split(",");
			for (int i = 0; i < cons.length; i++ ) {
				fillKeyValue(model.getConsequents(), cons[i]);
			}
		}
		
		return model;
	}
	
	private static void fillKeyValue(Map<String, String> map, String keyValue) {
		if ( map == null || keyValue == null ) {
			return;
		}
		
		String[] fields = keyValue.split("=");
		if ( fields.length == 2 ) {
			map.put(fields[0], fields[1]);
		}
	}
	
	@Override
	public void write(DataOutput dataOutput) throws IOException {
		WritableUtils.writeString(dataOutput, this.toJson());
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		String json = WritableUtils.readString(dataInput);
		VolvoModel model = new VolvoModel(json);
		this.copy(model);
	}

	@Override
	public int compareTo(VolvoModel model) {
		if ( model == null ) {
			return 1;
		}
		
		if ( this.confidence > model.getConfidence() ) {
			return 1;
		}
		else if ( this.confidence < model.getConfidence() ) {
			return -1;
		}
		return 0;
	}
}
