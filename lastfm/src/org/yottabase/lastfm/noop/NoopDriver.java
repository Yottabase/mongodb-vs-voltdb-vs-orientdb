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
//		System.out.println("code: " + user.getCode());
//		System.out.println("gender: "+ user.getGender());
//		System.out.println("age: " + user.getAge());
//		System.out.println("country: " + user.getCountry());
//		
//		System.out.println("==========================");
		
	}

	@Override
	public void insertListenedTrack(ListenedTrack listenedTrack) {
		
//		System.out.println("code: " + listenedTrack.getCode());
//		System.out.println("time: " + listenedTrack.getTime().toString());
//		System.out.println("artistId: " + listenedTrack.getArtistCode());
//		System.out.println("artistName: " + listenedTrack.getArtistName());
//		System.out.println("trackId: " + listenedTrack.getTrackCode());
//		System.out.println("trackName: " + listenedTrack.getTrackName());
//		
//		System.out.println("==========================");
//		
	}
	
}
