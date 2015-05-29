package org.yottabase.lastfm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.yottabase.lastfm.core.Config;
import org.yottabase.lastfm.core.DriverFactory;
import org.yottabase.lastfm.core.Driver;
import org.yottabase.lastfm.importer.ListenedTrack;
import org.yottabase.lastfm.importer.User;

public class BenchmarkMain {
	
	public static void main(String[] args) throws IOException {
		
		Config config = new Config();
		Properties properties = config.getProperties();
		
		List<Driver> drivers = initDrivers(properties);
		
		for(Driver driver: drivers){
			
			driver.initializeSchema();
			
			importDataset(properties.getProperty("dataset.path"), driver);
			
		}
	}
	
	public static List<Driver> initDrivers(Properties properties) throws IOException{
		String[] driversName = {"noop", "voltdb", "mongodb", "orientdb"};
		
		List<Driver> drivers = new ArrayList<Driver>();
		
		for(String driverName : driversName ){
			
			String enabledFlagKey = driverName + ".enabled";
			String driverFactoryKey = driverName + ".factory";
			
			if(properties.get(enabledFlagKey).equals("true")){
				
				String factoryClassName = properties.getProperty(driverFactoryKey);
				
				try {
					DriverFactory driverFactory = (DriverFactory) Class.forName(factoryClassName).newInstance();
					Driver driver = driverFactory.createService(properties);
					drivers.add(driver);
					
				} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
					e.printStackTrace();
				}
			}
		}
		
		return drivers;
	}
	
	public static void importDataset(String path, Driver driver){
		
		//TODO realizzare
		
		User user = new User();
		user.setCode("code name");
		
		ListenedTrack listenedTrack = new ListenedTrack();
		listenedTrack.setCode("track code");
		
		
		driver.insertUser(user);
		driver.insertListenedTrack(listenedTrack);
		
		
	}
	
	
}
