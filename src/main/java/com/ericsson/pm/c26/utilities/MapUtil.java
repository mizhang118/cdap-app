package com.ericsson.pm.c26.utilities;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class MapUtil {
	public static <K, V extends Comparable<? super V>> Map<K, V> sortByValue( Map<K, V> map ) {
		List<Map.Entry<K, V>> list = new LinkedList<>( map.entrySet() );
		Collections.sort( list, new Comparator<Map.Entry<K, V>>() {
			@Override
			public int compare( Map.Entry<K, V> o1, Map.Entry<K, V> o2 ) {
				return 0 - (o1.getValue()).compareTo(o2.getValue());
			}});
		
		Map<K, V> result = new LinkedHashMap<>();
		for (Map.Entry<K, V> entry : list) {
			result.put( entry.getKey(), entry.getValue() );
		}
		return result;
	}
	
	public static void main(String[] args) {
		Random random = new Random();
		Map<String, Integer> map = new HashMap<String, Integer>(1000);
		for( int i = 0; i < 100; i++ ) {
			map.put("A" + i, random.nextInt(1000));
		}
		
		map = MapUtil.sortByValue(map);
		
		for ( Map.Entry<String, Integer> entry : map.entrySet() ) {
			System.out.println(entry.getKey() + " -> " + entry.getValue());
		}
	}
}
