package com.ericsson.pm.c26.entities;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.gson.Gson;

public class VolvoTripTest {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void test() {
		String dataLine = "20805934,Burlingame,US,37.58612823,-122.3340225,3191282,94010,California,HumboldtRoad1134,2/18/2015 21:36,115,SouthSanFrancisco,US,37.64136505,-122.4006119,3180979,94080,California,SanFranciscoBayTrail138-168,2/18/2015 21:17,20906830,0,0,0,0,0,0,37.58612823,-122.4006119,37.64136505,-122.3340225,0,20906830,unassigned,2/18/2015-21:36,2/18/2015 21:17,20809";
		VolvoTrip trip = VolvoTrip.parse(dataLine);
		String json = trip.toJson();
		System.out.println(json);
		
		Gson gson = new Gson();
		VolvoTrip secondTrip = gson.fromJson(json, VolvoTrip.class);
		String secondJson = secondTrip.toJson();
		System.out.println(secondJson);
		Assert.assertTrue("Two trips should be same", json.equals(secondJson));
		
		VolvoTrip thirdTrip = new VolvoTrip();
		try { thirdTrip.copy(trip); } catch (Exception e) { e.printStackTrace(); }
		System.out.println(thirdTrip.toJson());
	}

}
