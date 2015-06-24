package org.yottabase.lastfm.core;

public interface DBFacadeFactory {

	/**
	 * Crea un oggetto che interagisce con uno specifico db
	 * @param properties
	 * @return
	 */
	public AbstractDBFacade createService(PropertyFile properties);
	
}
