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
import com.ericsson.pm.c26.entities.VolvoTrip;

import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.service.AbstractService;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;

public class C26TripService extends AbstractService {
	@Override
	protected void configure() {
		setName(C26AnalyticsApp.SERVICE_TRIP);
	    addHandler(new TripServiceHandler());
	}
	
	/**
	 * Handler which defines HTTP endpoints to access information stored in the {@code c26TripStore} Dataset.
	 */
	public static class TripServiceHandler extends AbstractHttpServiceHandler {
	    private static final Logger LOG = LoggerFactory.getLogger(TripServiceHandler.class);

	    // Annotation indicates that the pageViewStore custom DataSet is used in the Service
	    @UseDataSet("c26TripStore")
	    private C26TripDataset tripStore;

	    /**
	     * Queries the total number of vins
	     *
	     * <pre>{@code
	     *
	     * GET http://mzs-macbook-pro.local:11015/v3/namespaces/test/apps/c26Analytics/services/c26TripService/methods/trip/vin/count
	     * }</pre>
	     */
	    @GET
	    @Path("/trip/vin/count")
	    public void getVinCount(HttpServiceRequest request, HttpServiceResponder responder) {
	    	List<String> keys = tripStore.getAllKeys();
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
	     * GET http://mzs-macbook-pro.local:11015/v3/namespaces/test/apps/c26Analytics/services/c26TripService/methods/trip/vins
	     * }</pre>
	     */
	    @GET
	    @Path("/trip/vins")
	    public void getVins(HttpServiceRequest request, HttpServiceResponder responder) {
	    	List<String> keys = tripStore.getAllKeys();
	    	
	    	responder.sendJson(200, keys);
	    }
	    
	    /**
	     * Queries the total number of trips in all vins
	     *
	     * <pre>{@code
	     *
	     * GET http://mzs-macbook-pro.local:11015/v3/namespaces/test/apps/c26Analytics/services/c26TripService/methods/trip/count
	     * }</pre>
	     */
	    @GET
	    @Path("/trip/count")
	    public void getTripCount(HttpServiceRequest request, HttpServiceResponder responder) {
	    	List<String> keys = tripStore.getAllKeys();
	    	long count = 0;
	    	
	    	for(String key : keys) {
	    		count += tripStore.getDataCount(key);
	    	}
	    	
	    	responder.sendJson(200, count);
	    }
	    
	    /**
	     * Queries the trips in a given vin
	     *
	     * <pre>{@code
	     *
	     * GET http://mzs-macbook-pro.local:11015/v3/namespaces/test/apps/c26Analytics/services/c26TripService/methods/trips/{vin}
	     * }</pre>
	     */
	    @GET
	    @Path("/trips/{vin}")
	    public void getTripsByVin(HttpServiceRequest request, HttpServiceResponder responder, 
	    		                 @PathParam("vin") String vin) {
	    	
	    	Map<String, String> map = tripStore.getData(vin);
	    	Map<String, VolvoTrip> trips = new HashMap<String, VolvoTrip>();
			for (Map.Entry<String, String> entry : map.entrySet()) {
				VolvoTrip trip = (VolvoTrip) (new VolvoTrip()).toEntity(entry.getValue());
				trips.put(entry.getKey(), trip);
			}
	    	responder.sendJson(200, trips);
	    }

	    /**
	     * Queries the number of trips of a given vin. It expects GET request to a URL of the form:
	     *
	     * <pre>{@code
	     *
	     * GET http://mzs-macbook-pro.local:11015/v3/namespaces/test/apps/c26Analytics/services/c26TripService/methods/trip/[vin]/count
	     * }</pre>
	     *
	     * With the URI to query form in the GET body.
	     */
	    @GET
	    @Path("/trip/{vin}/count")
	    public void getTripCountByVin(HttpServiceRequest request, HttpServiceResponder responder,
	                                 @PathParam("vin") String vin) {
	    	long count = tripStore.getDataCount(vin);
	    	responder.sendJson(200, count);
	    }
	}
}
