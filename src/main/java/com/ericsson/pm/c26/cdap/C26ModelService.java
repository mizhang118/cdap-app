package com.ericsson.pm.c26.cdap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.pm.c26.entities.VolvoModel;
import com.google.gson.Gson;

import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.service.AbstractService;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;

public class C26ModelService extends AbstractService {
	@Override
	protected void configure() {
		setName(C26AnalyticsApp.SERVICE_MODEL);
	    addHandler(new ModelServiceHandler());
	}
	
	/**
	 * Handler which defines HTTP endpoints to access information stored in the {@code c26ModelStore} Dataset.
	 */
	public static class ModelServiceHandler extends AbstractHttpServiceHandler {
	    private static final Logger LOG = LoggerFactory.getLogger(ModelServiceHandler.class);

	    // Annotation indicates that the pageViewStore custom DataSet is used in the Service
	    @UseDataSet("c26ModelStore")
	    private C26TripDataset modelStore;

	    /**
	     * Queries the total number of vins
	     *
	     * <pre>{@code
	     *
	     * GET http://mzs-macbook-pro.local:11015/v3/namespaces/test/apps/c26Analytics/services/c26ModelService/methods/model/vin/count
	     * }</pre>
	     */
	    @GET
	    @Path("/model/vin/count")
	    public void getVinCount(HttpServiceRequest request, HttpServiceResponder responder) {
	    	List<String> keys = modelStore.getAllKeys();
	    	long count = 0;
	    	if ( keys != null ) {
	    		count = keys.size();
	    	}
	    	responder.sendJson(200, count);
	    }
	    
	    /**
	     * Queries the list of vins
	     *
	     * <pre>{@code
	     *
	     * GET http://mzs-macbook-pro.local:11015/v3/namespaces/test/apps/c26Analytics/services/c26ModelService/methods/model/vins
	     * }</pre>
	     */
	    @GET
	    @Path("/model/vins")
	    public void getVins(HttpServiceRequest request, HttpServiceResponder responder) {
	    	List<String> keys = modelStore.getAllKeys();
	    	
	    	responder.sendJson(200, keys);
	    }
	    
	    /**
	     * Queries the total number of models in all vins
	     *
	     * <pre>{@code
	     *
	     * GET http://mzs-macbook-pro.local:11015/v3/namespaces/test/apps/c26Analytics/services/c26ModelService/methods/model/count
	     * }</pre>
	     */
	    @GET
	    @Path("/model/count")
	    public void getModelCount(HttpServiceRequest request, HttpServiceResponder responder) {
	    	List<String> keys = modelStore.getAllKeys();
	    	long count = 0;
	    	
	    	for(String key : keys) {
	    		count += modelStore.getDataCount(key);
	    	}
	    	
	    	responder.sendJson(200, count);
	    }
	    
	    /**
	     * Queries the models in a given vin
	     *
	     * <pre>{@code
	     *
	     * GET http://mzs-macbook-pro.local:11015/v3/namespaces/test/apps/c26Analytics/services/c26ModelService/methods/model/{vin}
	     * }</pre>
	     */
	    @GET
	    @Path("/model/{vin}")
	    public void getModelsByVin(HttpServiceRequest request, HttpServiceResponder responder, 
	    		                 @PathParam("vin") String vin) {
	    	Map<String, String> map = modelStore.getData(vin);
	    	List<VolvoModel> models = new ArrayList<VolvoModel>();
	    	if ( map == null ) {
	    		responder.sendJson(200, models);
	    		return;
	    	}
	    	Gson gson = new Gson();
			for (Map.Entry<String, String> entry : map.entrySet()) {
				VolvoModel model = null;
				try {
					model = gson.fromJson(entry.getValue(), VolvoModel.class);
				}
				catch (Exception e) {
					LOG.error("Wrong JSON to create VolvoMode: {}", entry.getValue(), e);
					continue;
				}
				insertModel(models, model);
			}
	    	responder.sendJson(200, models);
	    }
	    
	    /**
	     * Queries the models in a given vin
	     *
	     * <pre>{@code
	     *
	     * GET http://mzs-macbook-pro.local:11015/v3/namespaces/test/apps/c26Analytics/services/c26ModelService/methods/model/query/{vin}?origin=xxx&timeOfDay=yyy&dayOfWeek=Monday&dayType=weekend
	     * }</pre>
	     */
	    @GET
	    @Path("/model/query/{vin}")
	    public void queryModelsByVin(HttpServiceRequest request, HttpServiceResponder responder, 
	    		                 @PathParam("vin") String vin,
	    		                 @QueryParam("origin") String origin,
	    		                 @QueryParam("timeOfDay") String timeOfDay,
	    		                 @QueryParam("dayOfWeek") String dayOfWeek,
	    		                 @QueryParam("dayType") String dayType) {
	    	//read parameters
	    	LOG.info("Model query parameters: vin=" + vin + ", origin=" + origin + ", timeOfDay=" + timeOfDay + ", dayOfWeek=" + dayOfWeek + ", dayType=" + dayType);
	    	
	    	Map<String, String> map = modelStore.getData(vin);
	    	List<VolvoModel> models = new ArrayList<VolvoModel>();
	    	if ( map == null ) {
	    		responder.sendJson(200, models);
	    		return;
	    	}	    	
	    	Gson gson = new Gson();
			for (Map.Entry<String, String> entry : map.entrySet()) {
				VolvoModel model = null;
				try {
					model = gson.fromJson(entry.getValue(), VolvoModel.class);
				}
				catch (Exception e) {
					LOG.error("Wrong JSON to create VolvoMode: {}", entry.getValue(), e);
					continue;
				}
				//filter models by parameter
				if ( origin != null ) {
					origin = origin.toLowerCase();
					
					String ori = model.getAntecedent("origin");
					if ( ori != null ) {
						ori = ori.toLowerCase();
						if ( ori.indexOf(origin) < 0 ) {
							continue;
						}
					}
					else {
						continue;
					}
				}
				if ( timeOfDay != null ) {
					timeOfDay = timeOfDay.toLowerCase();
					
					String tod = model.getAntecedent("timeOfDay");
					if ( tod != null ) {
						tod = tod.toLowerCase();
						if ( !tod.equals(timeOfDay) ) {
							continue;
						}
					}
					else {
						continue;
					}
				}
				if ( dayOfWeek != null ) {
					dayOfWeek = dayOfWeek.toLowerCase();
					String dow = model.getAntecedent("dayOfWeek");
					if ( dow != null ) {
						dow = dow.toLowerCase();
						if ( !dow.equals(dayOfWeek) ) {
							continue;
						}
					}
					else {
						continue;
					}
				}
				if ( dayType != null ) {
					dayType = dayType.toLowerCase();
					String dt = model.getAntecedent("dayType");
					if ( dt != null ) {
						dt = dt.toLowerCase();
						if ( !dt.equals(dayType) ) {
							continue;
						}
					}
					else {
						continue;
					}
				}
				
				//insert the model after it passes all filters
				insertModel(models, model);
			}
	    	responder.sendJson(200, models);
	    }

	    /**
	     * Queries the number of models of a given vin. It expects GET request to a URL of the form:
	     *
	     * <pre>{@code
	     *
	     * GET http://mzs-macbook-pro.local:11015/v3/namespaces/test/apps/c26Analytics/services/c26ModelService/methods/model/[vin]/count
	     * }</pre>
	     *
	     * With the URI to query form in the GET body.
	     */
	    @GET
	    @Path("/model/{vin}/count")
	    public void getModelCountByVin(HttpServiceRequest request, HttpServiceResponder responder,
	                                 @PathParam("vin") String vin) {
	    	long count = modelStore.getDataCount(vin);
	    	responder.sendJson(200, count);
	    }
	    
	    private void insertModel(List<VolvoModel> models, VolvoModel model) {
	    	//(1) handle null issue
	    	if ( model == null || models == null ) {
	    		return;
	    	}
	    	
	    	int size = models.size();
	    	
	    	//(2) handle empty list
	    	if ( size == 0 ) {
	    		models.add(model);
	    		return;
	    	}
	    	
	    	//(3) Insert model with largest confidence: add into head
	    	if ( model.getConfidence() > models.get(0).getConfidence() ) {
	    		models.add(0, model);
	    		return;
	    	}
	    	
	    	//(4) Insert model with smallest confidence: add into tail
	    	if ( model.getConfidence() <= models.get(size - 1).getConfidence() ) {
	    		models.add(size, model);
	    		return;
	    	}
	    	
	    	//(5) Insert model into middle
	    	int count = 1;
	    	while ( count < size ) {
	    		if ( model.getConfidence() > models.get(count).getConfidence() ) {
	    			models.add(count, model);
	    			return;
	    		}
	    		count++;
	    	}
	    }
	}
}
