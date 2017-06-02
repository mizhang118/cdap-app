package com.ericsson.pm.c26.cdap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

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
	     * GET http://mzs-macbook-pro.local:11015/v3/namespaces/test/apps/c26Analytics/services/c26ModelService/methods/models/{vin}
	     * }</pre>
	     */
	    @GET
	    @Path("/models/{vin}")
	    public void getfeaturesByVin(HttpServiceRequest request, HttpServiceResponder responder, 
	    		                 @PathParam("vin") String vin) {
	    	Map<String, String> map = modelStore.getData(vin);
	    	Map<String, VolvoModel> models = new HashMap<String, VolvoModel>();
	    	Gson gson = new Gson();
			for (Map.Entry<String, String> entry : map.entrySet()) {
				VolvoModel model = null;
				try {
					model = gson.fromJson(entry.getValue(), VolvoModel.class);
				}
				catch (Exception e) {
					LOG.error("Wrong JSON to create VolvoMode: {}", entry.getValue(), e);
				}
				models.put(entry.getKey(), model);
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
	    public void getFeatureCountByVin(HttpServiceRequest request, HttpServiceResponder responder,
	                                 @PathParam("vin") String vin) {
	    	long count = modelStore.getDataCount(vin);
	    	responder.sendJson(200, count);
	    }
	}
}
