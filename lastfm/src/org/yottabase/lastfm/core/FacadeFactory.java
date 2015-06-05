package org.yottabase.lastfm.core;

import java.util.Properties;

public interface FacadeFactory {

	/**
	 * Crea un oggetto che interagisce con uno specifico db
	 * @param properties
	 * @return
	 */
	public Facade createService(Properties properties);
	
}
