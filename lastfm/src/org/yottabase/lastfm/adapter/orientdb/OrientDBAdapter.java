package org.yottabase.lastfm.adapter.orientdb;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.yottabase.lastfm.core.AbstractDBFacade;
import org.yottabase.lastfm.importer.ListenedTrack;
import org.yottabase.lastfm.importer.User;

import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Parameter;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertexType;
import com.tinkerpop.gremlin.java.GremlinPipeline;

public class OrientDBAdapter extends AbstractDBFacade {
	
	private OrientGraph graph;

	public OrientDBAdapter(OrientGraph graph) {
		this.graph = graph;
	}

	@Override
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void initializeSchema() {	
		OrientVertexType userVertexClass = graph.createVertexType("User");
		userVertexClass.createProperty("userID", OType.STRING);
		userVertexClass.createProperty("gender", OType.STRING);
		userVertexClass.createProperty("age", OType.INTEGER);
		userVertexClass.createProperty("country", OType.STRING);
		userVertexClass.createProperty("signupDate", OType.DATE);
		
		OrientVertexType trackVertexClass = graph.createVertexType("Track");
		trackVertexClass.createProperty("trackID", OType.STRING);
		trackVertexClass.createProperty("name", OType.STRING);
		
		OrientVertexType artistVertexClass = graph.createVertexType("Artist");
		artistVertexClass.createProperty("artistID", OType.STRING);
		artistVertexClass.createProperty("name", OType.STRING);
		
		graph.createKeyIndex("userID", Vertex.class, new Parameter("type", "UNIQUE"), new Parameter("class", "User"));
		graph.createKeyIndex("trackID", Vertex.class, new Parameter("type", "UNIQUE"), new Parameter("class", "Track"));
		graph.createKeyIndex("artistID", Vertex.class, new Parameter("type", "UNIQUE"), new Parameter("class", "Artist"));
		
	}
	
	@Override
	public void close() {
		graph.shutdown();
		
	}

	@Override
	public void insertUser(User user) {
		@SuppressWarnings("unused")
		Vertex userVertex = graph.addVertex(
				"class:User",
				"userID", 		user.getCode(),
				"gender", 		user.getGender(),
				"age", 			user.getAge(),
				"country", 		user.getCountry(),
				"signup_date", 	user.getSignupDateAsJavaDate());
		
		graph.commit();
		
	}

	@Override
	public void insertListenedTrack(ListenedTrack listenedTrack) {
		Vertex user = graph.getVertexByKey("User.userID", listenedTrack.getCode());
		Vertex artist = graph.getVertexByKey("Artist.artistID", listenedTrack.getArtistCode());
		Vertex track = graph.getVertexByKey("Track.trackID", listenedTrack.getTrackCode());
		boolean newSingRel = false;	// new pair <artist,track>
		
		if (artist == null) {
			newSingRel = true;
			
			artist = graph.addVertex(
				"class:Artist",
				"artistID", listenedTrack.getArtistCode(),
				"name", 	listenedTrack.getArtistName());
		}
		
		if (track == null) {
			newSingRel = true;
			
			track = graph.addVertex(
				"class:Track",
				"trackID", 	listenedTrack.getTrackCode(),
				"name", 	listenedTrack.getTrackName());
		}
		
		if (newSingRel)
			graph.addEdge(null, artist, track, "Sing");
		
		graph.addEdge(null, user, track, "Listen").setProperty("time", listenedTrack.getTimeAsJavaDate());

		graph.commit();
		
	}

	@Override
	public void countArtists() {
		long numArtists = graph.countVertices("Artist");
		writer.write( String.valueOf(numArtists) );
	}

	@Override
	public void countTracks() {
		long numTracks = graph.countVertices("Track");
		writer.write( String.valueOf(numTracks) );
	}

	@Override
	public void countUsers() {
		long numUsers = graph.countVertices("User");
		writer.write( String.valueOf(numUsers) );
	}

	@Override
	public void countEntities() {
		long numVertices = graph.countVertices();
		writer.write( String.valueOf(numVertices) );
	}

	@Override
	@SuppressWarnings("unchecked")
	public void averageNumberListenedTracksPerUser(boolean uniqueTrack) {
		String querySQL = 
				"SELECT avg(listenings) FROM ("
					+ "SELECT out_Listen.size() as listenings "
					+ "FROM User" + 
				")";
		 
		for (Vertex v : (Iterable<Vertex>) graph.command(new OCommandSQL(querySQL)).execute())
			writer.write( String.valueOf(v.getProperty("avg")) ); 	
	}

	@Override
	@SuppressWarnings("unchecked")
	public void averageNumberSungTracksPerArtist() {
		String querySQL = 
			  "SELECT avg(out_Sing.size()) "
			+ "FROM Artist";
		
		for (Vertex v : (Iterable<Vertex>) graph.command(new OCommandSQL(querySQL)).execute())
			writer.write( String.valueOf(v.getProperty("avg")) );		
	}

	@Override
	@SuppressWarnings("unchecked")
	public void usersChart(int n, boolean top, boolean uniqueTrack) {		
		String querySQL = 
				  "SELECT userID, gender, age, country, signup_date, out_Listen.size() AS listenings "
				+ "FROM User "
				+ "ORDER BY listenings " + ((top) ? "ASC" : "DESC") + " "
				+ "LIMIT " + n;
		
		for (Vertex v : (Iterable<Vertex>) graph.command(new OCommandSQL(querySQL)).execute()) {
			writer.write(
					v.getProperty("userID"), 
					v.getProperty("gender"), 
					String.valueOf(v.getProperty("age")), 
					v.getProperty("country"), 
					String.valueOf(v.getProperty("signup_date")), 
					String.valueOf(v.getProperty("listenings")) );
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	public void tracksChart(int n, boolean top, boolean uniqueTracks) {
		String querySQL = 		
				  "SELECT trackID, name, in_Listen.size() AS listenings "
				+ "FROM Track "
				+ "ORDER BY listenings " + ((top) ? "ASC" : "DESC") + " "
				+ "LIMIT " + n;
		
		for (Vertex v : (Iterable<Vertex>) graph.command(new OCommandSQL(querySQL)).execute()) {
			writer.write(
					v.getProperty("trackID"), 
					v.getProperty("name"), 
					String.valueOf(v.getProperty("listenings")) );
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	public void artistsChart(int n, boolean top, boolean uniqueTracks) {
		String querySQL = 
				  "SELECT in_Sing, in_Listen.size() AS listenings "
				+ "FROM Track "
				+ "ORDER BY listenings " + ((top) ? "ASC" : "DESC") + " "
				+ "LIMIT " + n;
		
		for (Vertex v : (Iterable<Vertex>) graph.command(new OCommandSQL(querySQL)).execute()) {
			Vertex artist = v.getEdges(Direction.IN, "Sing").iterator().next().getVertex(Direction.OUT);
			
			writer.write( 
					artist.getProperty("artistID"), 
					artist.getProperty("name"), 
					String.valueOf(v.getProperty("listenings")) );
		}
	}

	@Override
	public void artistByCode(String artistCode) {
		Vertex artist = graph.getVertexByKey("Artist.artistID", artistCode);
		
		writer.write(
				artist.getProperty("artistID"),
				artist.getProperty("name"));
	}

	@Override
	public void artistByName(String artistName) {
		Element artist = 
				new GremlinPipeline<Vertex, Vertex>(graph.getVerticesOfClass("Artist"))
				.has("name", artistName)
				.next();
		
		writer.write(
				artist.getProperty("artistID"),
				artist.getProperty("name"));
	}

	@Override
	public void usersByAgeRange(int lowerBound, int upperBound) {
		GremlinPipeline<Vertex,? extends Element> users = 
				new GremlinPipeline<Vertex, Vertex>(graph.getVerticesOfClass("User"))
				.interval("age", lowerBound, upperBound);

		for (Element user : users) {
			writer.write(
					user.getProperty("userID"), 
					user.getProperty("gender"), 
					String.valueOf(user.getProperty("age")), 
					user.getProperty("country"), 
					String.valueOf(user.getProperty("signup_date")) );
		}
	}

	@Override
	public void tracksSungByArtist(String artistCode) {
		Vertex artist = graph.getVertexByKey("Artist.artistID", artistCode);
		Iterable<Vertex> artistTracks = artist.getVertices(Direction.OUT, "Sing");
		
		for (Vertex track : artistTracks) {
			writer.write(
					track.getProperty("trackID"), 
					track.getProperty("name") );
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	public void usersCountByCountry() {
		Map<String,Integer> counts = (HashMap<String,Integer>)
				new GremlinPipeline<Vertex, Vertex>(graph.getVerticesOfClass("User"))
				.property("country").groupCount().cap().next();
		
		for (Entry<String, Integer> e : counts.entrySet()) {
			writer.write( e.getKey(), String.valueOf(e.getValue()) );
		}
	}
	
	@Override
	@SuppressWarnings("unchecked")
	public void usersCountByCountryAndGender() {
		String querySQL = 
				  "SELECT country, gender, count(*) "
				+ "FROM User "
				+ "WHERE country IS NOT NULL AND gender IS NOT NULL "
				+ "GROUP BY country, gender";
		
		for (Vertex v : (Iterable<Vertex>) graph.command(new OCommandSQL(querySQL)).execute()) {
			writer.write( 
					v.getProperty("country") + "\t" +
					v.getProperty("gender") + "\t" +
					String.valueOf(v.getProperty("count")) );
		}
	}

}
