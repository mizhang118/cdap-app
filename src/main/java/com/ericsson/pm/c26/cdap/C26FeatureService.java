package com.ericsson.pm.c26.cdap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.pm.c26.entities.VolvoFeature;

import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.service.AbstractService;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;

public class C26FeatureService extends AbstractService {
	@Override
	protected void configure() {
		setName(C26AnalyticsApp.SERVICE_FEATURE);
	    addHandler(new FeatureServiceHandler());
	}
	
	/**
	 * Handler which defines HTTP endpoints to access information stored in the {@code c26FeatureStore} Dataset.
	 */
	public static class FeatureServiceHandler extends AbstractHttpServiceHandler {
	    private static final Logger LOG = LoggerFactory.getLogger(FeatureServiceHandler.class);

	    // Annotation indicates that the pageViewStore custom DataSet is used in the Service
	    @UseDataSet("c26FeatureStore")
	    private C26TripDataset featureStore;

	    /**
	     * Queries the total number of vins
	     *
	     * <pre>{@code
	     *
	     * GET http://mzs-macbook-pro.local:11015/v3/namespaces/test/apps/c26Analytics/services/c26FeatureService/methods/feature/vin/count
	     * }</pre>
	     */
	    @GET
	    @Path("/feature/vin/count")
	    public void getVinCount(HttpServiceRequest request, HttpServiceResponder responder) {
	    	List<String> keys = featureStore.getAllKeys();
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
	     * GET http://mzs-macbook-pro.local:11015/v3/namespaces/test/apps/c26Analytics/services/c26FeatureService/methods/feature/vins
	     * }</pre>
	     */
	    @GET
	    @Path("/feature/vins")
	    public void getVins(HttpServiceRequest request, HttpServiceResponder responder) {
	    	List<String> keys = featureStore.getAllKeys();
	    	
	    	responder.sendJson(200, keys);
	    }
	    
	    /**
	     * Queries the total number of features in all vins
	     *
	     * <pre>{@code
	     *
	     * GET http://mzs-macbook-pro.local:11015/v3/namespaces/test/apps/c26Analytics/services/c26FeatureService/methods/feature/count
	     * }</pre>
	     */
	    @GET
	    @Path("/feature/count")
	    public void getFeatureCount(HttpServiceRequest request, HttpServiceResponder responder) {
	    	List<String> keys = featureStore.getAllKeys();
	    	long count = 0;
	    	
	    	for(String key : keys) {
	    		count += featureStore.getDataCount(key);
	    	}
	    	
	    	responder.sendJson(200, count);
	    }
	    
	    /**
	     * Queries the features in a given vin
	     *
	     * <pre>{@code
	     *
	     * GET http://mzs-macbook-pro.local:11015/v3/namespaces/test/apps/c26Analytics/services/c26FeatureService/methods/feature/{vin}
	     * }</pre>
	     */
	    @GET
	    @Path("/feature/{vin}")
	    public void getfeaturesByVin(HttpServiceRequest request, HttpServiceResponder responder, 
	    		                 @PathParam("vin") String vin) {
	    	Map<String, String> map = featureStore.getData(vin);
	    	Map<String, VolvoFeature> features = new HashMap<String, VolvoFeature>();
			for (Map.Entry<String, String> entry : map.entrySet()) {
				VolvoFeature feature = (VolvoFeature) (new VolvoFeature()).toEntity(entry.getValue());
				features.put(entry.getKey(), feature);
			}
	    	responder.sendJson(200, features);
	    }

	    /**
	     * Queries the number of features of a given vin. It expects GET request to a URL of the form:
	     *
	     * <pre>{@code
	     *
	     * GET http://mzs-macbook-pro.local:11015/v3/namespaces/test/apps/c26Analytics/services/c26FeatureService/methods/feature/[vin]/count
	     * }</pre>
	     *
	     * With the URI to query form in the GET body.
	     */
	    @GET
	    @Path("/trip/{vin}/count")
	    public void getFeatureCountByVin(HttpServiceRequest request, HttpServiceResponder responder,
	                                 @PathParam("vin") String vin) {
	    	long count = featureStore.getDataCount(vin);
	    	responder.sendJson(200, count);
	    }
	}
}
