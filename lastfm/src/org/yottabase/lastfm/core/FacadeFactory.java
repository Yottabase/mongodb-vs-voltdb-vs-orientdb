package org.yottabase.lastfm.core;

public interface FacadeFactory {

	/**
	 * Crea un oggetto che interagisce con uno specifico db
	 * @param properties
	 * @return
	 */
	public Facade createService(PropertyFile properties);
	
}
