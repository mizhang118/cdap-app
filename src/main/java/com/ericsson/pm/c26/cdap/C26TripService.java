package com.ericsson.pm.c26.cdap;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.service.AbstractService;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;

public class C26TripService extends AbstractService {
	  @Override
	  protected void configure() {
	    setName("C26TripService");
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
	     * GET http://[host]:[port]/v3/namespaces/test/apps/c26Analytics/services/TripService/methods/vin/count
	     * }</pre>
	     */
	    @GET
	    @Path("/vin/count")
	    public void getVinCount(HttpServiceRequest request, HttpServiceResponder responder) {
	      long count = 0;
	      responder.sendJson(200, count);
	    }
	    
	    /**
	     * Queries the total number of trips in all vins
	     *
	     * <pre>{@code
	     *
	     * GET http://[host]:[port]/v3/namespaces/test/apps/c26Analytics/services/TripService/methods/trip/count
	     * }</pre>
	     */
	    @GET
	    @Path("/trip/count")
	    public void getTripCount(HttpServiceRequest request, HttpServiceResponder responder) {
	      long count = 0;
	      responder.sendJson(200, count);
	    }

	    /**
	     * Queries the number of trips of a given vin. It expects GET request to a URL of the form:
	     *
	     * <pre>{@code
	     *
	     * GET http://[host]:[port]/v3/namespaces/test/apps/c26Analytics/services/TripService/methods/trip/[vin]/count
	     * }</pre>
	     *
	     * With the URI to query form in the GET body.
	     */
	    @GET
	    @Path("/trip/{vin}/count")
	    public void getTripCountByVin(HttpServiceRequest request, HttpServiceResponder responder,
	                                 @PathParam("vin") String vin) {
	      long count = 0;
	      responder.sendJson(200, count);
	    }
	  }
	}
