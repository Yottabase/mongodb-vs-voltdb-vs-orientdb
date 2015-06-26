package org.yottabase.lastfm.adapter.voltdb;

import java.io.File;
import java.io.IOException;

import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.yottabase.lastfm.adapter.voltdb.procedure.ArtistsChart;
import org.yottabase.lastfm.adapter.voltdb.procedure.AverageNumberListenedTracksPerUser;
import org.yottabase.lastfm.adapter.voltdb.procedure.AverageNumberSungTracksPerArtist;
import org.yottabase.lastfm.adapter.voltdb.procedure.Count;
import org.yottabase.lastfm.adapter.voltdb.procedure.GetArtist;
import org.yottabase.lastfm.adapter.voltdb.procedure.GetTracksByArtist;
import org.yottabase.lastfm.adapter.voltdb.procedure.InsertListenedTrackRecursive;
import org.yottabase.lastfm.adapter.voltdb.procedure.TracksChart;
import org.yottabase.lastfm.adapter.voltdb.procedure.UsersByAgeRange;
import org.yottabase.lastfm.adapter.voltdb.procedure.UsersChart;
import org.yottabase.lastfm.core.AbstractDBFacade;
import org.yottabase.lastfm.importer.ListenedTrack;
import org.yottabase.lastfm.importer.User;

public class VoltDBAdapter extends AbstractDBFacade{

	private static final String PROCEDURE_ADHOC = "@AdHoc";
	
	private final File procedureJar = new File("resources/voltdbProcedure.jar");
	
	private final String[] proceduresNames = {
		InsertListenedTrackRecursive.class.getCanonicalName(),
		Count.class.getCanonicalName(),
		AverageNumberListenedTracksPerUser.class.getCanonicalName(),
		AverageNumberSungTracksPerArtist.class.getCanonicalName(),
		UsersChart.class.getCanonicalName(),
		TracksChart.class.getCanonicalName(),
		ArtistsChart.class.getCanonicalName(),
		GetArtist.class.getCanonicalName(),
		UsersByAgeRange.class.getCanonicalName(),
		GetTracksByArtist.class.getCanonicalName(),
	};
	
	private Client client;
	
	public VoltDBAdapter(Client client) {
		this.client = client;
	}

	@Override
	public void initializeSchema() {

		//pulisce il database
		String dqlClean = new StringBuilder()
			.append("DROP TABLE User IF EXISTS; ")
			.append("DROP TABLE Artist IF EXISTS; ")
			.append("DROP TABLE Track IF EXISTS; ")
			.append("DROP TABLE ListenedTrack IF EXISTS; ")
			.toString();
		
		for (int i = 0; i < proceduresNames.length; i++) {
			String name = proceduresNames[i];
			dqlClean += "DROP PROCEDURE " + name + " IF EXISTS;";
		}
		
		String dql = new StringBuilder()
			//crea tabella User
			.append("CREATE TABLE User ( ")
			.append("Code VARCHAR NOT NULL, Gender VARCHAR(1), Age TINYINT, Country VARCHAR, SignupDate TIMESTAMP,")
			.append("PRIMARY KEY (Code)")
			.append(");")
			
			//crea tabella Artist
			.append("CREATE TABLE Artist ( ")
			.append("Code VARCHAR NOT NULL, Name VARCHAR,")
			.append("PRIMARY KEY (Code)")
			.append(");")
			
			//crea tabella Track
			.append("CREATE TABLE Track ( ")
			.append("Code VARCHAR NOT NULL, Name VARCHAR, ArtistCode VARCHAR,")
			.append("PRIMARY KEY (Code)")
			.append(");")
			
			//crea tabella ListenedTrack
			.append("CREATE TABLE ListenedTrack ( ")
			.append("Time TIMESTAMP, TrackCode VARCHAR, UserCode VARCHAR,")
			.append("PRIMARY KEY (Time, TrackCode, UserCode)")
			.append(");")
			
			.toString();
		
		
		String createProcedures = "";
		for (int i = 0; i < proceduresNames.length; i++) {
			String name = proceduresNames[i];
			createProcedures += "CREATE PROCEDURE FROM CLASS " + name + ";";
		}
		
		try {
			
			this.client.callProcedure( PROCEDURE_ADHOC,	dqlClean);
			this.client.callProcedure( PROCEDURE_ADHOC,	dql);
			this.client.updateClasses(procedureJar, "");
			this.client.callProcedure( PROCEDURE_ADHOC,	createProcedures);
			
		} catch (IOException | ProcCallException e) {
			e.printStackTrace();
		}
		
	}
	
	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void insertUser(User user) {
		try {
			this.client.callProcedure(
				"User.INSERT", 
				user.getCode(),
				user.getGender(),
				user.getAge(),
				user.getCountry(),
				user.getSignupDateAsJavaDate()
			);
		} catch (IOException | ProcCallException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void insertListenedTrack(ListenedTrack listenedTrack) {
		try {
			this.client.callProcedure(
				"InsertListenedTrackRecursive", 
				listenedTrack.getCode(),
				listenedTrack.getTimeAsJavaDate(),
				listenedTrack.getArtistCode(),
				listenedTrack.getArtistName(),
				listenedTrack.getTrackCode(),
				listenedTrack.getTrackName()
			);
		} catch (IOException | ProcCallException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void countArtists() {
		try {
			ClientResponse response = this.client.callProcedure( "Count",	"Artists" );
			if (response.getStatus() == ClientResponse.SUCCESS) {
				long count = response.getResults()[0].asScalarLong();
				this.writer.write(Long.toString(count));
			}
		} catch (IOException | ProcCallException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void countTracks() {
		try {
			ClientResponse response = this.client.callProcedure( "Count",	"Artists" );
			if (response.getStatus() == ClientResponse.SUCCESS) {
				long count = response.getResults()[0].asScalarLong();
				this.writer.write(Long.toString(count));
			}
		} catch (IOException | ProcCallException e) {
			e.printStackTrace();
		}
		
	}

	@Override
	public void countUsers() {
		try {
			ClientResponse response = this.client.callProcedure( "Count",	"Users" );
			if (response.getStatus() == ClientResponse.SUCCESS) {
				long count = response.getResults()[0].asScalarLong();
				this.writer.write(Long.toString(count));
			}
		} catch (IOException | ProcCallException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void countEntities() {
		try {
			ClientResponse response = this.client.callProcedure( "Count",	"Artists+Tracks+Users" );
			if (response.getStatus() == ClientResponse.SUCCESS) {
				long count = response.getResults()[0].asScalarLong();
				this.writer.write(Long.toString(count));
			}
		} catch (IOException | ProcCallException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void averageNumberListenedTracksPerUser(boolean uniqueTrack) {
		try {
			ClientResponse response = this.client.callProcedure( "AverageNumberListenedTracksPerUser" );
			if (response.getStatus() == ClientResponse.SUCCESS) {
				long count = response.getResults()[0].asScalarLong();
				this.writer.write(Long.toString(count));
			}
		} catch (IOException | ProcCallException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void averageNumberSungTracksPerArtist() {
		try {
			ClientResponse response = this.client.callProcedure( "AverageNumberSungTracksPerArtist" );
			if (response.getStatus() == ClientResponse.SUCCESS) {
				long count = response.getResults()[0].asScalarLong();
				this.writer.write(Long.toString(count));
			}
		} catch (IOException | ProcCallException e) {
			e.printStackTrace();
		}
		
	}

	@Override
	public void usersChart(int n, boolean top, boolean uniqueTrack) {
		try {
			ClientResponse response = this.client.callProcedure( "UsersChart", n,  (top ? "DESC" : "ASC") );
			if (response.getStatus() == ClientResponse.SUCCESS) {
				System.out.println(response.getResults()[0].toFormattedString());
				
			}
		} catch (IOException | ProcCallException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void tracksChart(int n, boolean top, boolean uniqueTracks) {
		try {
			ClientResponse response = this.client.callProcedure( "TracksChart", n,  (top ? "DESC" : "ASC") );
			if (response.getStatus() == ClientResponse.SUCCESS) {
				System.out.println(response.getResults()[0].toFormattedString());
				
			}
		} catch (IOException | ProcCallException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void artistsChart(int n, boolean top, boolean uniqueTracks) {
		try {
			ClientResponse response = this.client.callProcedure( "ArtistsChart", n,  (top ? "DESC" : "ASC") );
			if (response.getStatus() == ClientResponse.SUCCESS) {
				System.out.println(response.getResults()[0].toFormattedString());
				
			}
		} catch (IOException | ProcCallException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void artistByCode(String artistCode) {
		try {
			ClientResponse response = this.client.callProcedure( "GetArtist", "CODE", artistCode );
			if (response.getStatus() == ClientResponse.SUCCESS) {
				System.out.println(response.getResults()[0].toFormattedString());
				
			}
		} catch (IOException | ProcCallException e) {
			e.printStackTrace();
		}
		
	}

	@Override
	public void artistByName(String artistName) {
		try {
			ClientResponse response = this.client.callProcedure( "GetArtist", "NAME", artistName );
			if (response.getStatus() == ClientResponse.SUCCESS) {
				System.out.println(response.getResults()[0].toFormattedString());
				
			}
		} catch (IOException | ProcCallException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void usersByAgeRange(int lowerBound, int upperBound) {
		try {
			ClientResponse response = this.client.callProcedure( "UsersByAgeRange", lowerBound, upperBound );
			if (response.getStatus() == ClientResponse.SUCCESS) {
				System.out.println(response.getResults()[0].toFormattedString());
				
			}
		} catch (IOException | ProcCallException e) {
			e.printStackTrace();
		}
		
	}

	@Override
	public void tracksSungByArtist(String artistCode) {
		try {
			ClientResponse response = this.client.callProcedure( "GetTracksByArtist", artistCode );
			if (response.getStatus() == ClientResponse.SUCCESS) {
				System.out.println(response.getResults()[0].toFormattedString());
				
			}
		} catch (IOException | ProcCallException e) {
			e.printStackTrace();
		}
		
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
