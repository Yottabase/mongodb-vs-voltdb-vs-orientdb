package org.yottabase.lastfm.noop;

import org.yottabase.lastfm.core.Driver;
import org.yottabase.lastfm.importer.ListenedTrack;
import org.yottabase.lastfm.importer.User;

public class NoopDriver implements Driver{

	@Override
	public void initializeSchema() {
		System.out.println("crea schema");
	}
	
	@Override
	public void insertUser(User user) {
		System.out.println("dummy insert profile: " + user);
		
	}

	@Override
	public void insertListenedTrack(ListenedTrack listenedTrack) {
		System.out.println("dummy insert listened track: " + listenedTrack);
	}
	
}
