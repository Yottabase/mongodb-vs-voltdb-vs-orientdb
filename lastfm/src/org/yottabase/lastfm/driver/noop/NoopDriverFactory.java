package org.yottabase.lastfm.driver.noop;

import java.util.Properties;

import org.yottabase.lastfm.core.Driver;
import org.yottabase.lastfm.core.DriverFactory;

public class NoopDriverFactory implements DriverFactory {

	@Override
	public Driver createService(Properties properties) {
		
		return new NoopDriver();
	}

}
