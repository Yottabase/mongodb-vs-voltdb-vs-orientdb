package org.yottabase.lastfm.core;

import org.yottabase.lastfm.importer.ListenedTrack;
import org.yottabase.lastfm.importer.Profile;

public interface QueryFacade {
	
	/**
	 * Aggiunge un profilo al database
	 * @param profile
	 */
	public void insertProfile(Profile profile);
	
	/**
	 * Aggiunge una traccia ascoltata al database
	 * @param listenedTrack
	 */
	public void insertListenedTrack(ListenedTrack listenedTrack);
	
	

}
