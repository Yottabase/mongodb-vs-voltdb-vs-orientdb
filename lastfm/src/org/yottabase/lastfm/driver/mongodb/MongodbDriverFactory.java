package org.yottabase.lastfm.driver.mongodb;

import java.util.Properties;

import org.yottabase.lastfm.core.Driver;
import org.yottabase.lastfm.core.DriverFactory;

public class MongodbDriverFactory implements DriverFactory {

	@Override
	public Driver createService(Properties properties) {
		
		return new MongodbDriver();
	}

}
