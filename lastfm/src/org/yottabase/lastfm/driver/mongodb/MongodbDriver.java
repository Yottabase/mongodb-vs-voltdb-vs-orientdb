package org.yottabase.lastfm.driver.mongodb;

import org.bson.Document;
import org.yottabase.lastfm.core.Driver;
import org.yottabase.lastfm.importer.ListenedTrack;
import org.yottabase.lastfm.importer.User;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;


public class MongodbDriver extends Driver {
	private MongoClient mongoClient;
	private MongoDatabase db;

	private final static String DATABASE = "lastfm";
	private final static String COLLECTIONUSERS = "users";
	private final static String COLLECTIONARTISTS = "artists";
	private final static String COLLECTIONTRACKS = "tracks";
	private final static String COLLECTIONLISTENEDTRACKS = "listenedTracks";
	private boolean artistDuplicate;
	private boolean trackDuplicate;

	public MongodbDriver(MongoClient client) {
		this.mongoClient = client;
	}

	@Override
	public void initializeSchema() {
		db = mongoClient.getDatabase(DATABASE);

		db.drop();

		System.out.println("crea schema");

		/*
		 * index options
		 */

		IndexOptions indOpt = new IndexOptions();
		indOpt.unique(true);

		// create tracks index

		db.getCollection(COLLECTIONTRACKS).createIndex(
				new Document("trackId", 1), indOpt);
		

		// create artist index

		db.getCollection(COLLECTIONARTISTS).createIndex(
				(new Document("artistId", 1)), indOpt);

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
		artistDuplicate = false;
		trackDuplicate = false;
		FindIterable<Document> iterable;

		/*
		 * insert tracks
		 */
		iterable = db.getCollection(COLLECTIONTRACKS).find(
				new Document("trackId", listenedTrack.getTrackCode()));
		
		if( iterable.first() != null )
			trackDuplicate = true;
		
		/*
		 * insert tracks
		 */
		if (!trackDuplicate) {
			db.getCollection(COLLECTIONTRACKS).insertOne(
					new Document("trackId", listenedTrack.getTrackCode())
					.append("trackName", listenedTrack.getTrackName())		
					.append("artistId", listenedTrack.getArtistCode())
					);
		}


		/*
		 * insert artists
		 */
		iterable = db.getCollection(COLLECTIONARTISTS).find(
				new Document("artistId", listenedTrack.getArtistCode()));

		if( iterable.first() != null )
				artistDuplicate = true;


		if (!artistDuplicate) {
			db.getCollection(COLLECTIONARTISTS).insertOne( new Document("artistId", listenedTrack.getArtistCode())
															.append("artistName", listenedTrack.getArtistName()));
		}

		
		/*
		 * listenedTracks
		 */
		db.getCollection(COLLECTIONLISTENEDTRACKS).insertOne(
				new Document("userId", listenedTrack.getCode())
				.append("trackId", listenedTrack.getTrackCode())
				.append("time", listenedTrack.getTimeAsJavaDate())
				);
		
		
		/*
		 * associo il brano all'artista
		 */
		if (!trackDuplicate) {
			BasicDBObject ArtistTracks = new BasicDBObject();
			ArtistTracks.put("trackId", listenedTrack.getTrackCode());

			db.getCollection(COLLECTIONARTISTS)
					.updateOne(
							new BasicDBObject("artistId",
									listenedTrack.getArtistCode()),
							new BasicDBObject("$push", ArtistTracks));
		}
	}

	@Override
	public void countArtists() {
		this.writer.write(String.valueOf(db.getCollection(COLLECTIONARTISTS).count()));
	}

	@Override
	public void countTracks() {
		System.out.println(db.getCollection(COLLECTIONTRACKS).count());
	}

	@Override
	public void countUsers() {
		System.out.println(db.getCollection(COLLECTIONUSERS).count());	
	}

	@Override
	public void countEntities() {
		// TODO Auto-generated method stub
		//fai somma dei risultati dei tre metodi precedenti
		
	}

	@Override
	public void averageNumberListenedTracksPerUser(boolean uniqueTrack) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void averageNumberSungTracksPerArtist(boolean uniqueTrack) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void usersChart(int n, boolean top, boolean uniqueTrack) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void tracksChart(int n, boolean top, boolean uniqueTracks) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void artistsChart(int n, boolean top, boolean uniqueTracks) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void tracksListenedTogether(int n) {
		// TODO Auto-generated method stub
		
	}

}
