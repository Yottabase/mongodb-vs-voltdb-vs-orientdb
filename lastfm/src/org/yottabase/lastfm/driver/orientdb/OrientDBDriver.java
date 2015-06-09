package org.yottabase.lastfm.driver.orientdb;

import org.yottabase.lastfm.core.Driver;
import org.yottabase.lastfm.importer.ListenedTrack;
import org.yottabase.lastfm.importer.User;

import com.orientechnologies.orient.core.metadata.schema.OType;
import com.tinkerpop.blueprints.Parameter;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertexType;

public class OrientDBDriver implements Driver {
	
	private OrientGraph graph;

	public OrientDBDriver(OrientGraph graph) {
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
		Vertex track = graph.getVertexByKey("Track.trackID", listenedTrack.getTrackCode());;
		
		if (artist == null) {
			artist = graph.addVertex(
				"class:Artist",
				"artistID", listenedTrack.getArtistCode(),
				"name", 	listenedTrack.getArtistName());
		}
		
		if (track == null) {
			track = graph.addVertex(
				"class:Track",
				"trackID", 	listenedTrack.getTrackCode(),
				"name", 	listenedTrack.getTrackName());
			
			graph.addEdge(null, artist, track, "Sing");
		}
		
		graph.addEdge(null, user, track, "Listen").setProperty("time", listenedTrack.getTimeAsJavaDate());

		graph.commit();
		
	}

}
