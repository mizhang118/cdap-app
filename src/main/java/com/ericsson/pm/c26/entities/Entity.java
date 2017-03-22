/**
 * @author Ming Zhang
 * created at 3/17/2017
 * 
 * parent entity bean that makes conversion between JSON string and java bean
 */

package com.ericsson.pm.c26.entities;

import java.io.Serializable;
import java.lang.reflect.Method;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

public class Entity implements Serializable {
	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(Entity.class);
	
	public Entity() {
	}
	
	public Entity(String json) {
		try {
			Gson gson = new Gson();
			Entity entity = gson.fromJson(json, this.getClass());
			copy(entity);
		}
		catch (Exception e) {
			LOG.error("Wrong JSON format: {}", json, e);
		}
	}

	public String toJson() {
		String json = null;
		try {
			Gson gson = new Gson();
			json = gson.toJson(this);
		}
		catch (Exception e) {
			LOG.error("Failed to convert an Entity to JSON", e);
		}
		
		return json;
	}
	
	public void copy (Entity entity) {
        Method[] gettersAndSetters = entity.getClass().getMethods();

        try {
        	for (int i = 0; i < gettersAndSetters.length; i++) {
        		String methodName = gettersAndSetters[i].getName();
        		if(methodName.startsWith("get")) {
        			this.getClass().getMethod(methodName.replaceFirst("get", "set") , gettersAndSetters[i]
        					.getReturnType()).invoke(this, gettersAndSetters[i].invoke(entity));
        		}else if(methodName.startsWith("is") ) {
        			this.getClass().getMethod(methodName.replaceFirst("is", "set") ,  gettersAndSetters[i]
        					.getReturnType()  ).invoke(this, gettersAndSetters[i].invoke(entity));
        		}
        	}
        }
        catch (Exception e) {
        	//exception happens get/is method exists in entity, but set method does not exist
        	//just silently copy the bean without reporting exceptions
        }
    }
	
	@Override
	public String toString() {
		return toJson();
	}
}
