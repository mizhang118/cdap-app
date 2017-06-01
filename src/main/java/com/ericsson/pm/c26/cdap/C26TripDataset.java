/**
 * @author Ming Zhang
 * created at 3/16/2017
 * 
 * Customer dataset stores c26 trip raw data
 */

package com.ericsson.pm.c26.cdap;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.batch.RecordScannable;
import co.cask.cdap.api.data.batch.RecordScanner;
import co.cask.cdap.api.data.batch.Split;
import co.cask.cdap.api.data.batch.SplitReader;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.lib.AbstractDataset;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.module.EmbeddedDataset;
import co.cask.cdap.api.dataset.table.Delete;
import co.cask.cdap.api.dataset.table.Get;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scan;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;

import com.ericsson.pm.c26.entities.VolvoFeature;
import com.ericsson.pm.c26.entities.VolvoTrip;
import com.google.common.reflect.TypeToken;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A custom-defined Dataset is used to track page views by IP addresses.
 */
public class C26TripDataset extends AbstractDataset
							implements RecordScannable<KeyValue<String, Map<String, String>>> {
	private static final Logger LOG = LoggerFactory.getLogger(C26TripDataset.class);
	
	public static long TRAIN_INTERVAL = 1000 * 60 * 60; // milliseconds of one hour

	// Define the underlying table
	private Table table;

	public C26TripDataset(DatasetSpecification spec, @EmbeddedDataset("trips") Table table) {
		super(spec.getName(), table);
		this.table = table;
	}
	
	/**
	 * add a string into a vehicle VIN_ID
	 *
	 * @param String data
	 */
	public void addData(String rowKey, String key, String data) {
		table.put(new Put(rowKey).add(key, data));;
	}

	/**
	 * add a trip into a vehicle VIN_ID
	 *
	 * @param Trip raw data of a trip
	 */
	public void addTrip(VolvoTrip trip) {
		//table.increment(new Increment(logInfo.getIp(), logInfo.getUri(), 1L));
		table.put(new Put(trip.getVin()).add(trip.getId(), trip.toJson()));;
	}
	
	/**
	 * add a feature into a vehicle VIN_ID
	 *
	 * @param Trip feature data
	 */
	public void addFeature(VolvoFeature feature) {
		//table.increment(new Increment(logInfo.getIp(), logInfo.getUri(), 1L));
		table.put(new Put(feature.getVehicleId()).add(feature.getId(), feature.toJson()));;
	}
	
	public void addModel(String consequent, String antecedent, double confidence) {
		LOG.info("consequent is {} and antecedent is {} and confident is " + confidence, consequent, antecedent);
		
		//at first filter out all models that do not have destination in its consequent.
		
	}
	
	/**
	 * Get a data string from a specified VIN_ID.
	 *
	 * @param VIN_ID used to look for all trips of the vehicle
	 * @return all trips of a vehicle
	 */
	public void deleteData(String vin) {
		Delete delete = new Delete(vin);
		this.table.delete(delete);
	}

	/**
	 * Get a data string from a specified VIN_ID.
	 *
	 * @param VIN_ID used to look for all trips of the vehicle
	 * @return all trips of a vehicle
	 */
	public Map<String, String> getData(String vin) {
		Row row = this.table.get(new Get(vin));
		Map<String, String> trips = getStringData(row);
		return trips;
	}

	/**
	 * Get the total number of visited pages viewed from a specified IP address.
	 *
	 * @param vin used to look for visited pages counts
	 * @return the number of visited pages
	 */
	public long getDataCount(String vin) {
		Row row = this.table.get(new Get(vin));
		if (row.isEmpty()) {
			return 0;
		}
		int count = 0;
		for (Map.Entry<byte[], byte[]> entry : row.getColumns().entrySet()) {
			count += Bytes.toLong(entry.getValue());
		}
		return count;
	}
	
	public List<String> getARFeatures(String vin) {
		List<String> list = new ArrayList<String>();
		
		Map<String, String> map = getData(vin);
		for (Map.Entry<String, String> entry : map.entrySet()) {
			VolvoFeature feature = new VolvoFeature(entry.getValue());
			list.add(feature.asSparkFriendlyFeatureVector());
		}		
		
		return list;
	}
	
	public List<String> getAllKeys() {
		List<String> list = new ArrayList<String>();
		
		Scan scan = new Scan(null, null);
		Scanner scanner = table.scan(scan);
		Row row = null;
		while ( (row = scanner.next() ) != null ) {
			byte[] rowkey = row.getRow();
			list.add(Bytes.toString(rowkey));
		}
		
		return list;
	}
	
	public List<String> getVinForTrain() {
		List<String> list = new ArrayList<String>();
		long trainInterval = TRAIN_INTERVAL;
		
		Scan scan = new Scan(null, null);
		Scanner scanner = table.scan(scan);
		Row row = null;
		while ( (row = scanner.next() ) != null ) {
			byte[] rowkey = row.getRow();
			String vin = Bytes.toString(rowkey);
			
			String timestampStr = row.getString(Bytes.toBytes("timestamp"));
			long timestamp = 0l;
			try { timestamp = Long.parseLong(timestampStr); } catch (Exception e) {}
			LOG.debug("vin {} has timestamp of {}", vin, timestamp);
			long current = System.currentTimeMillis();
			if ( (current - timestamp) < trainInterval ) {
				list.add(Bytes.toString(rowkey));
			}
		}
		
		return list;
	}

	@Override
	public Type getRecordType() {
		return new TypeToken<KeyValue<String, Map<String, String>>>() { }.getType();
	}

	@Override
	public List<Split> getSplits() {
		return table.getSplits();
	}

	@Override
	public RecordScanner<KeyValue<String, Map<String, String>>> createSplitRecordScanner(Split split) {
		return new RecordScanner<KeyValue<String, Map<String, String>>>() {
			private SplitReader<byte[], Row> splitReader;

			@Override
			public void initialize(Split split) throws InterruptedException {
				this.splitReader = table.createSplitReader(split);
				this.splitReader.initialize(split);
			}

			@Override
			public boolean nextRecord() throws InterruptedException {
				return this.splitReader.nextKeyValue();
			}

			@Override
			public KeyValue<String, Map<String, String>> getCurrentRecord() throws InterruptedException {
				String vin = Bytes.toString(this.splitReader.getCurrentKey());
				Row row = this.splitReader.getCurrentValue();
				Map<String, String> pageCount = getStringData(row);
				return new KeyValue<String, Map<String, String>>(vin, pageCount);
			}

			@Override
			public void close() {
				this.splitReader.close();
			}
		};
	}

	private Map<String, String> getStringData(Row row) {
		if (row == null || row.isEmpty()) {
			return null;
		}
		Map<String, String> trips = new HashMap<String, String>();
		for (Map.Entry<byte[], byte[]> entry : row.getColumns().entrySet()) {
			trips.put(Bytes.toString(entry.getKey()), Bytes.toString(entry.getValue()));
		}
		return trips;
	}
}

