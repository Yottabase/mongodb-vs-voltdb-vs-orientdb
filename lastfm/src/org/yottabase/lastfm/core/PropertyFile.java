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
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		catch (IOException e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}

	public String get(String eregName) {
		return this.propertiesFile.getProperty(eregName);

	}

}
