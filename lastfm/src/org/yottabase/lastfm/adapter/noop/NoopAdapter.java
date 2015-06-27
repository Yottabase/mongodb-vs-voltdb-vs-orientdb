package org.yottabase.lastfm.adapter.noop;

import org.yottabase.lastfm.core.AbstractDBFacade;
import org.yottabase.lastfm.importer.ListenedTrack;
import org.yottabase.lastfm.importer.User;

public class NoopAdapter extends AbstractDBFacade{

	@Override
	public void initializeSchema() {
		System.out.println("crea schema");
		this.writer.write("prova");
	}
	
	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public void insertUser(User user) {
//		System.out.println("code: " + user.getCode());
//		System.out.println("gender: "+ user.getGender());
//		System.out.println("age: " + user.getAge());
//		System.out.println("country: " + user.getCountry());
//		System.out.println("signupDate: " + user.getSignupDate());
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
		
	}

	@Override
	public void countArtists() {
		this.writer.write("10");
		
	}

	@Override
	public void countTracks() {
		this.writer.write("10");
		
	}

	@Override
	public void countUsers() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void countEntities() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void averageNumberListenedTracksPerUser() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void averageNumberSungTracksPerArtist() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void usersChart(int n, boolean top) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void tracksChart(int n, boolean top) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void artistsChart(int n, boolean top) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void artistByCode(String artistCode) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void artistByName(String artistName) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void usersByAgeRange(int lowerBound, int upperBound) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void tracksSungByArtist(String artistCode) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void usersCountByCountry() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void usersCountByCountryAndGender() {
		// TODO Auto-generated method stub
		
	}
	
}
