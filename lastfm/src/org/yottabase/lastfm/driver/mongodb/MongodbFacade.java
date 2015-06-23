package org.yottabase.lastfm.driver.mongodb;

import static java.util.Arrays.asList;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.yottabase.lastfm.core.Facade;
import org.yottabase.lastfm.importer.ListenedTrack;
import org.yottabase.lastfm.importer.User;

import com.mongodb.BasicDBObject;
import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;



public class MongodbFacade extends Facade {
	private MongoClient mongoClient;
	private MongoDatabase db;

	private final static String DATABASE = "lastfmTest";
	private final static String COLLECTIONUSERS = "users";
	private final static String COLLECTIONARTISTS = "artists";

	public MongodbFacade(MongoClient client) {
		this.mongoClient = client;
		db = mongoClient.getDatabase(DATABASE);
	}

	@Override
	public void initializeSchema() {
		db.drop();

		System.out.println("crea schema");

		/*
		 * index options
		 */

		IndexOptions uniqueCostraint = new IndexOptions();
		uniqueCostraint.unique(true);
		
		IndexOptions noUniqueCostraint = new IndexOptions();
		noUniqueCostraint.unique(false);
		

		/*
		 * l'indice RALLENTA notevolmente gli inserimenti perchè ad ogni scrittura deve essere aggiornato
		
		// create artist index

		
		db.getCollection(COLLECTIONARTISTS).createIndex(
				(new Document("artistId", 1)), uniqueCostraint);
		
		//create user index
		 
		db.getCollection(COLLECTIONUSERS).createIndex(
				(new Document("code", 1)), uniqueCostraint);
		
		//db.users.createIndex( { "tracks.trackId": 1, "tracks.time": 1 } )
		
		db.getCollection(COLLECTIONUSERS).createIndex(
				(new Document("tracks.trackId", 1).append("tracks.time", 1)), noUniqueCostraint);
		
		
		//db.artists.createIndex( { "songs.trackId": 1, "songs.trackName": 1 } )

		db.getCollection(COLLECTIONARTISTS).createIndex(
				(new Document("songs.trackId", 1).append("songs.trackName", 1)), uniqueCostraint);////////////verifica perche da errore con uniquecostr
				 */
	}

	@Override
	public void insertUser(User user) {
		
		Document userDoc = new Document("code", user.getCode());
		if(user.getGender()!=null)
			userDoc.append("gender", user.getGender());
		if(user.getAge()!=null)
			userDoc.append("age", user.getAge());
		if(user.getCountry()!=null)
			userDoc.append("country", user.getCountry());
		if(user.getSignupDateAsJavaDate()!=null)
			userDoc.append("singupDate", user.getSignupDateAsJavaDate());
		
		db.getCollection(COLLECTIONUSERS).insertOne(userDoc);
	}
	
	/*
	 * con user.tracks
	 * @see org.yottabase.lastfm.core.Facade#insertListenedTrack(org.yottabase.lastfm.importer.ListenedTrack)
	 */
	public void insertListenedTrack(ListenedTrack listenedTrack) {
		
		/*
		 * update artist
		 */
		Bson artist = new Document("artistId", listenedTrack.getArtistCode());
		
		BasicDBObject artistTracks = new BasicDBObject("trackId", listenedTrack.getTrackCode());
		
		if(listenedTrack.getTrackName()!=null)
			artistTracks.put("trackName", listenedTrack.getTrackName());
		
		BasicDBObject updateQuery =	new BasicDBObject("$push", new BasicDBObject("songs",artistTracks));
		
		Document update = db.getCollection(COLLECTIONARTISTS).findOneAndUpdate(artist,updateQuery);
		
		/*
		 * insert and update 
		 */
		if(update == null){
			db.getCollection(COLLECTIONARTISTS).insertOne( new Document("artistId", listenedTrack.getArtistCode())
			.append("artistName", listenedTrack.getArtistName()));
			
			db.getCollection(COLLECTIONARTISTS).updateOne(artist,updateQuery);
		}
		
		/*
		 * update listenedTracks users
		 */
		BasicDBObject trackObject = new BasicDBObject();
		trackObject.put("trackId", listenedTrack.getTrackCode());
		
		if(listenedTrack.getTimeAsJavaDate()!=null)
			trackObject.put("time", listenedTrack.getTimeAsJavaDate());

		db.getCollection(COLLECTIONUSERS).updateOne(
				new BasicDBObject("code", listenedTrack.getCode()),
				new BasicDBObject("$push", new BasicDBObject("tracks",
						trackObject)));
		
	}


	@Override
	public void countArtists() {
		
		this.writer.write(String.valueOf(db.getCollection(COLLECTIONARTISTS).count()));
			
	}

	@SuppressWarnings("unchecked")
	@Override
	public void countTracks() {
		/*console query
		 * 		db.runCommand ( { distinct: "artists", key: "songs" } )
		 */

		Document explodeTracks = new Document("distinct", "artists").append("key", "songs.trackId");
		
		Document distinctTracks = db.runCommand(explodeTracks);
		
		List<String> count = new LinkedList<String>();
		count.addAll((Collection<? extends String>) distinctTracks.get("values"));
		
		this.writer.write(Integer.toString(count.size()));
//		System.out.println(count.size());
		
	}

	@Override
	public void countUsers() {
		
		this.writer.write(Long.toString(db.getCollection(COLLECTIONUSERS).count()));
//		System.out.println("user : " + db.getCollection(COLLECTIONUSERS).count());
	}

	@Override
	public void countEntities() {
		
//		int countEntities; 
		
	}

	@Override
	public void averageNumberListenedTracksPerUser(boolean uniqueTrack) {
		
		/*console query
		 * db.users.aggregate( [ { $project: { "_id":1, "code": 1, "total": { $size: "$tracks" } } },{$group:{_id: "null",avgQuantity: { $avg: "$total" }}} ] )
		 */
		Document groupByTracks = new Document("$project", new Document("code", 1).append("total", new Document("$size", "$tracks")));
		Document avg = new Document("$group", new Document("_id", null).append("avg", new Document("$avg", "$total")));
		
		AggregateIterable<Document> iterable = db.getCollection(COLLECTIONUSERS).aggregate(asList(groupByTracks,avg));
		iterable.forEach(new Block<Document>() {
			public void apply(final Document document) {
				System.out.println(document.toJson());
					
			}
		});
		
		
	}

	@Override
	public void averageNumberSungTracksPerArtist(boolean uniqueTrack) {
		
		/*console query
		 * db.artists.aggregate( [ { $project: { "_id":1, "artistId": 1, "total": { $size: "$songs" } } },{$group:{_id: "null",avgQuantity: { $avg: "$total" }}} ] )
		 */
		
		Document groupByTracks = new Document("$project", new Document("artistId", 1).append("total", new Document("$size", "$songs")));
		Document avg = new Document("$group", new Document("_id", null).append("avg", new Document("$avg", "$total")));
		
		AggregateIterable<Document> iterable = db.getCollection(COLLECTIONARTISTS).aggregate(asList(groupByTracks,avg));
		iterable.forEach(new Block<Document>() {
			public void apply(final Document document) {
				System.out.println(document.toJson());
					
			}
		});
		
		
	}

	@Override
	public void usersChart(int n, boolean top, boolean uniqueTrack) {
		int order = 1;
		if(top)
			order = -1;
			
		
		/*console query
		 * db.users.aggregate( [ { $project: { "_id":0, "code": 1, "numberListenedTracks": { $size: "$tracks" } } } ] )
		 */
		
		Document groupByTracks = new Document("$project", new Document("code", 1).append("_id", 0).append("total", new Document("$size", "$tracks")));
		Document sort = new Document("$sort", new Document("total",order));
		Document limit = new Document("$limit", n);
		
		AggregateIterable<Document> iterable = db.getCollection(COLLECTIONUSERS).aggregate(asList(groupByTracks,sort,limit));
		iterable.forEach(new Block<Document>() {
			public void apply(final Document document) {
				System.out.println(document.toJson());
					
			}
		});
		
		
	}

	@Override
	public void tracksChart(int n, boolean top, boolean uniqueTracks) {
		int order = 1;
		if(top)
			order = -1;
		
		Document explodeTracks = new Document("$unwind", "$tracks");
		Document groupBy = new Document("$group", new Document("_id", "$tracks.trackId").append("total", new Document("$sum", 1)));
		Document sort = new Document("$sort", new Document("total",order));
		Document limit = new Document("$limit", n);
		
		AggregateIterable<Document> iterable = db.getCollection(COLLECTIONUSERS).aggregate(asList(explodeTracks,groupBy,sort,limit));
		
		iterable.forEach(new Block<Document>() {
			public void apply(final Document document) {
				System.out.println(document.toJson());
					
			}
		});
		
	}

	@Override

	public void artistsChart(int n, boolean top, boolean uniqueTracks) {
		int order = 1;
		if(top)
			order = -1;
			
		
		/*console query
		 * db.users.aggregate( [ { $project: { "_id":0, "artistId": 1, "total": { $size: "$tracks" } } } ] )
		 */
		
		Document groupByTracks = new Document("$project", new Document("artistId", 1).append("_id", 0).append("total", new Document("$size", "$songs")));
		Document sort = new Document("$sort", new Document("total",order));
		Document limit = new Document("$limit", n);
		
		AggregateIterable<Document> iterable = db.getCollection(COLLECTIONARTISTS).aggregate(asList(groupByTracks,sort,limit));
		iterable.forEach(new Block<Document>() {
			public void apply(final Document document) {
				System.out.println(document.toJson());
					
			}
		});
		
		
	}

	@Override
	public void tracksListenedTogether(int n) {
		// TODO Auto-generated method stub
		
	}

}
