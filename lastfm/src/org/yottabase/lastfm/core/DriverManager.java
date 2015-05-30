package org.yottabase.lastfm.core;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class DriverManager {
	
	private Properties properties;
	
	public DriverManager(Properties properties) {
		super();
		this.properties = properties;
	}

	public Properties getProperties() {
		return properties;
	}

	public void setProperties(Properties properties) {
		this.properties = properties;
	}

	public List<Driver> getDrivers(){
		String[] driversName = properties.getProperty("supported_drivers").split(",");

		List<Driver> drivers = new ArrayList<Driver>();

		for (String driverName : driversName) {

			String enabledFlagKey = driverName + ".enabled";
			String driverFactoryKey = driverName + ".factory";

			if (properties.get(enabledFlagKey).equals("true")) {

				String factoryClassName = properties
						.getProperty(driverFactoryKey);

				try {
					DriverFactory driverFactory = (DriverFactory) Class
							.forName(factoryClassName).newInstance();
					Driver driver = driverFactory.createService(properties);
					drivers.add(driver);

				} catch (InstantiationException | IllegalAccessException
						| ClassNotFoundException e) {
					e.printStackTrace();
				}
			}
		}

		return drivers;
	}

}
