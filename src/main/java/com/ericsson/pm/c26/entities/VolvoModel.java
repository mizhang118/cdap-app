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
	
	private Map<String, String> elements = new HashMap<String, String>();
	private double support;
	private double confidence;
	private double lift;
	
	public VolvoModel() {
		super();
	}
	
	public VolvoModel(String json) {
		super(json);
	}
	
	public void putElement(String key, String value) {
		this.elements.put(key, value);
	}
	
	public String getElement(String key) {
		return this.elements.get(key);
	}
	
	public Map<String, String> getElements() {
		return elements;
	}

	public void setElements(Map<String, String> elements) {
		this.elements = elements;
	}

	public double getSupport() {
		return support;
	}

	public void setSupport(double support) {
		this.support = support;
	}

	public double getConfidence() {
		return confidence;
	}

	public void setConfidence(double confidence) {
		this.confidence = confidence;
	}

	public double getLift() {
		return lift;
	}

	public void setLift(double lift) {
		this.lift = lift;
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
