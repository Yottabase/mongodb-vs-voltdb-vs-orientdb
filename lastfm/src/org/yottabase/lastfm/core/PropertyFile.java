package org.yottabase.lastfm.core;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertyFile {

	private Properties propertiesFile = new Properties();

	public PropertyFile(String propFileName) {
		InputStream inputStream;
		
		try {
			inputStream = new FileInputStream(propFileName);
			propertiesFile.load(inputStream);
		} 
		catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public PropertyFile(InputStream inputStream) {
		try {
			propertiesFile.load(inputStream);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public String get(String prop) {
		return this.propertiesFile.getProperty(prop);

	}

}
