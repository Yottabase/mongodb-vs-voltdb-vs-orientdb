package org.yottabase.lastfm.core;

import org.yottabase.lastfm.importer.ListenedTrack;
import org.yottabase.lastfm.importer.User;

public interface Driver {
	
	/**
	 * Inizializza il database 
	 */
	public void initializeSchema();
	
	/**
	 * Aggiunge un profilo al database
	 * @param user
	 */
	public void insertUser(User user);
	
	/**
	 * Aggiunge una traccia ascoltata al database
	 * @param listenedTrack
	 */
	public void insertListenedTrack(ListenedTrack listenedTrack);
	
	

}
