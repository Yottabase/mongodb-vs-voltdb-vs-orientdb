package org.yottabase.lastfm.adapter.voltdb;

import java.io.IOException;

import org.voltdb.client.Client;
import org.voltdb.client.NullCallback;
import org.yottabase.lastfm.importer.ListenedTrack;
import org.yottabase.lastfm.importer.User;

public class VoltDBAdapterAsyncInsert extends VoltDBAdapter {

	private Client client;
	
	public VoltDBAdapterAsyncInsert(Client client) {
		super(client);
		this.client = client;
	}
	
	public VoltDBAdapterAsyncInsert(Client client, String procedureFilepath) {
		super(client, procedureFilepath);
		this.client = client;
	}

	@Override
	public void insertUser(User user) {
		try {
			this.client.callProcedure(
				new NullCallback(),
				"User.INSERT", 
				user.getCode(),
				user.getGender(),
				user.getAge(),
				user.getCountry(),
				user.getSignupDateAsJavaDate()
			);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}

	@Override
	public void insertListenedTrack(ListenedTrack listenedTrack) {
		try {
			this.client.callProcedure(
				new NullCallback(),
				"InsertListenedTrackRecursive", 
				listenedTrack.getCode(),
				listenedTrack.getTimeAsJavaDate(),
				listenedTrack.getArtistCode(),
				listenedTrack.getArtistName(),
				listenedTrack.getTrackCode(),
				listenedTrack.getTrackName()
			);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	

}
