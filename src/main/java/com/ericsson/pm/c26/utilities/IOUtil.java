package com.ericsson.pm.c26.utilities;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IOUtil {
	private static final Logger LOG = LoggerFactory.getLogger(IOUtil.class);
	
	
	
	public static void saveFile(String file, String content) {
		try {
			FileUtils.writeStringToFile(new File(file), content);
		} catch (IOException e) {
			LOG.error("IOException thrown while saving data to file", e);
		}		
	}
	
	public static void saveFile2(String file, String content) {
		try {
			FileWriter writer = new FileWriter(new File(file));
			writer.write(content);
			writer.close();
		} catch (IOException e) {
			LOG.error("IOException thrown while saving data to file", e);
		}		
	}
	
	public static String readFile(String file) {
		String content = null;		
		try {
			content = FileUtils.readFileToString(new File(file));
		} catch (IOException e) {
			LOG.error("IOException thrown while reading data to file", e);
		}
		
		return content;
	}
	
	public static String readFile2(String file) {
		String content = null;		
		try {
			BufferedReader reader = new BufferedReader(new FileReader(new File(file)));
			String line = null;
			StringBuilder builder = new StringBuilder();
			while ( (line=reader.readLine()) != null ) {
				builder.append(line + "\n");
			}
			reader.close();
			
			if ( builder.length() > 0 ) {
				content = builder.toString();
			}
		} catch (IOException e) {
			LOG.error("IOException thrown while reading data to file", e);
		}
		
		return content;
	}
}
