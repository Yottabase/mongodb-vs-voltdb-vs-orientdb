package org.yottabase.lastfm.core;

import java.util.Properties;

public interface DriverFactory {

	/**
	 * Crea un oggetto che interagisce con uno specifico db
	 * @param properties
	 * @return
	 */
	public Driver createService(Properties properties);
	
}
