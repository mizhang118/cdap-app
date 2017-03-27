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
import co.cask.cdap.api.dataset.table.Get;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Table;

import com.ericsson.pm.c26.entities.VolvoFeature;
import com.ericsson.pm.c26.entities.VolvoTrip;
import com.google.common.reflect.TypeToken;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A custom-defined Dataset is used to track page views by IP addresses.
 */
public class C26TripDataset extends AbstractDataset
							implements RecordScannable<KeyValue<String, Map<String, String>>> {

	// Define the underlying table
	private Table table;

	public C26TripDataset(DatasetSpecification spec, @EmbeddedDataset("trips") Table table) {
		super(spec.getName(), table);
		this.table = table;
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

	/**
	 * Get a trip from a specified VIN_ID.
	 *
	 * @param VIN_ID used to look for all trips of the vehicle
	 * @return all trips of a vehicle
	 */
	public Map<String, String> getTrips(String vin) {
		Row row = this.table.get(new Get(vin));
		Map<String, String> trips = getTrips(row);
		return trips;
	}

	/**
	 * Get the total number of visited pages viewed from a specified IP address.
	 *
	 * @param vin used to look for visited pages counts
	 * @return the number of visited pages
	 */
	public long getTripCount(String vin) {
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
				String ip = Bytes.toString(this.splitReader.getCurrentKey());
				Row row = this.splitReader.getCurrentValue();
				Map<String, String> pageCount = getTrips(row);
				return new KeyValue<String, Map<String, String>>(ip, pageCount);
			}

			@Override
			public void close() {
				this.splitReader.close();
			}
		};
	}

	private Map<String, String> getTrips(Row row) {
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

