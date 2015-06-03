package org.yottabase.lastfm.driver.mongodb;

import org.bson.Document;
import org.yottabase.lastfm.core.Driver;
import org.yottabase.lastfm.importer.ListenedTrack;
import org.yottabase.lastfm.importer.User;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;

public class MongodbDriver implements Driver {
	private static MongoDatabase db;
	private static MongoClient mongoClient;

	private final static String DATABASE = "lastfm";
	private final static String COLLECTIONUSERS = "users";
	private final static String COLLECTIONTRACKS = "tracks";

	@Override
	public void initializeSchema() {
		mongoClient = new MongoClient();
		db = mongoClient.getDatabase(DATABASE);
		db.drop();
		System.out.println("crea schema");
	}

	@Override
	public void insertUser(User user) {

		db.getCollection(COLLECTIONUSERS).insertOne(
				new Document("code", user.getCode())
						.append("gender", user.getGender())
						.append("age", user.getAge())
						.append("country", user.getCountry())
						.append("singupDate", user.getSignupDateAsJavaDate()));
	}

	@Override
	public void insertListenedTrack(ListenedTrack listenedTrack) {

		db.getCollection(COLLECTIONTRACKS).insertOne(
				new Document("code", listenedTrack.getCode())
						.append("time", listenedTrack.getTimeAsJavaDate())
						.append("artistId", listenedTrack.getArtistCode())
						.append("artistName", listenedTrack.getArtistName())
						.append("trackId", listenedTrack.getTrackCode())
						.append("trackName", listenedTrack.getTrackName()));

		/**
		 * associo il brano all'utente che lo ha ascoltato
		 */
		BasicDBObject trackObject = new BasicDBObject();
		trackObject.put("trackId", listenedTrack.getTrackCode());

		db.getCollection(COLLECTIONUSERS).updateOne(
				new BasicDBObject("code", listenedTrack.getCode()),
				new BasicDBObject("$push", new BasicDBObject("tracks",
						trackObject)));

	}

}
