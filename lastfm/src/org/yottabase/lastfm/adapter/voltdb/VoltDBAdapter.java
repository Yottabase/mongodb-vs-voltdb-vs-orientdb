package org.yottabase.lastfm.adapter.voltdb;

import java.io.File;
import java.io.IOException;

import org.voltdb.VoltTable;
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
import org.yottabase.lastfm.adapter.voltdb.procedure.UsersStats;
import org.yottabase.lastfm.core.AbstractDBFacade;
import org.yottabase.lastfm.importer.ListenedTrack;
import org.yottabase.lastfm.importer.User;

public class VoltDBAdapter extends AbstractDBFacade{

	private static final String PROCEDURE_ADHOC = "@AdHoc";
	
	private File procedureJar = new File("resources/voltdbProcedure.jar");
	
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
		UsersStats.class.getCanonicalName(),
	};
	
	private Client client;
	
	public VoltDBAdapter(Client client) {
		this.client = client;
	}
	
	public VoltDBAdapter(Client client, String procedureFilepath) {
		this.client = client;
		this.procedureJar = new File(procedureFilepath);
	}

	@Override
	public void initializeSchema() {

		//pulisce il database
		String dqlClean = new StringBuilder()
			.append("DROP VIEW TrackChart IF EXISTS; ")
			.append("DROP TABLE User IF EXISTS; ")
			.append("DROP TABLE Artist IF EXISTS; ")
			.append("DROP TABLE Track IF EXISTS; ")
			.append("DROP TABLE ListenedTrack IF EXISTS; ")
			.append("DROP INDEX ListenedTrack_TrackCode IF EXISTS; ")
			.append("DROP INDEX ListenedTrack_UserCode IF EXISTS; ")
			.append("DROP INDEX Track_ArtistCode IF EXISTS; ")
			.toString();
		
		for (int i = 0; i < proceduresNames.length; i++) {
			String name = proceduresNames[i];
			dqlClean += "DROP PROCEDURE " + name + " IF EXISTS;";
		}
		
		String dql = new StringBuilder()
			//crea tabella User
			.append("CREATE TABLE User ( ")
			.append("Code VARCHAR(40) NOT NULL, Gender VARCHAR(1), Age TINYINT, Country VARCHAR(50), SignupDate TIMESTAMP,")
			.append("PRIMARY KEY (Code)")
			.append(");")
			
			//crea tabella Artist
			.append("CREATE TABLE Artist ( ")
			.append("Code VARCHAR(40) NOT NULL, Name VARCHAR(280),")
			.append("PRIMARY KEY (Code)")
			.append(");")
			
			//crea tabella Track
			.append("CREATE TABLE Track ( ")
			.append("Code VARCHAR(40) NOT NULL, Name VARCHAR(280), ArtistCode VARCHAR(40),")
			.append("PRIMARY KEY (Code)")
			.append(");")
			.append("CREATE INDEX Track_ArtistCode ON Track ( ArtistCode );")
			
			//crea tabella ListenedTrack
			.append("CREATE TABLE ListenedTrack ( ")
			.append("Time TIMESTAMP, TrackCode VARCHAR(40), UserCode VARCHAR(15)")
			//.append("PRIMARY KEY (Time, TrackCode, UserCode)") unusued
			.append(");")
			//.append("CREATE INDEX ListenedTrack_TrackCode ON ListenedTrack ( TrackCode );") unusued
			.append("CREATE INDEX ListenedTrack_UserCode ON ListenedTrack ( UserCode );")
			
			//crea vista TrackChart
			.append("CREATE VIEW TrackChart (code, num) AS SELECT l.trackCode, COUNT(*) FROM listenedTrack l GROUP BY l.trackCode")
			
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
			ClientResponse response = this.client.callProcedure( "Count",	"Tracks" );
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
	public void averageNumberListenedTracksPerUser() {
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
	public void usersChart(int n, boolean top) {
		try {
			ClientResponse response = this.client.callProcedure( "UsersChart", n,  (top ? "DESC" : "ASC") );
			if (response.getStatus() == ClientResponse.SUCCESS) {
				VoltTable table = response.getResults()[0];
		        while (table.advanceRow()) {
		        	this.writer.write(
	        			table.getString(0), 
	        			table.getString(1), 
	        			Long.toString(table.getLong(2)),
	        			table.getString(3),
	        			table.getTimestampAsSqlTimestamp(4).toString(),
	        			Long.toString(table.getLong(5))
		        	);
		        }
			}
		} catch (IOException | ProcCallException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void tracksChart(int n, boolean top) {
		try {
			ClientResponse response = this.client.callProcedure( "TracksChart", n,  (top ? "DESC" : "ASC") );
			if (response.getStatus() == ClientResponse.SUCCESS) {
				VoltTable table = response.getResults()[0];
		        while (table.advanceRow()) {
		        	this.writer.write(table.getString(0), table.getString(1), Long.toString(table.getLong(2)));
		        }
			}
		} catch (IOException | ProcCallException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void artistsChart(int n, boolean top) {
		try {
			ClientResponse response = this.client.callProcedure( "ArtistsChart", n,  (top ? "DESC" : "ASC") );
			if (response.getStatus() == ClientResponse.SUCCESS) {
				VoltTable table = response.getResults()[0];
		        while (table.advanceRow()) {
		        	this.writer.write(table.getString(0), table.getString(1), Long.toString(table.getLong(2)));
		        }
				
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
				VoltTable table = response.getResults()[0];
		        while (table.advanceRow()) {
		        	this.writer.write(table.getString(0), table.getString(1));
		        }
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
				VoltTable table = response.getResults()[0];
		        while (table.advanceRow()) {
		        	this.writer.write(table.getString(0), table.getString(1));
		        }
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
				VoltTable table = response.getResults()[0];
		        while (table.advanceRow()) {
		        	this.writer.write(
	        			table.getString(0), 
	        			table.getString(1), 
	        			Long.toString(table.getLong(2)),
	        			table.getString(3),
	        			table.getTimestampAsSqlTimestamp(4).toString()
		        	);
		        }
				
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
				VoltTable table = response.getResults()[0];
		        while (table.advanceRow()) {
		        	this.writer.write(table.getString(0), table.getString(1));
		        }
			}
		} catch (IOException | ProcCallException e) {
			e.printStackTrace();
		}
		
	}

	@Override
	public void usersCountByCountry() {
		try {
			ClientResponse response = this.client.callProcedure( "UsersStats", "Country");
			if (response.getStatus() == ClientResponse.SUCCESS) {
				VoltTable table = response.getResults()[0];
		        while (table.advanceRow()) {
		        	this.writer.write(table.getString(0), Long.toString(table.getLong(1)));
		        }
			}
		} catch (IOException | ProcCallException e) {
			e.printStackTrace();
		}
		
	}

	@Override
	public void usersCountByCountryAndGender() {
		try {
			ClientResponse response = this.client.callProcedure( "UsersStats", "Country+Gender");
			if (response.getStatus() == ClientResponse.SUCCESS) {
				VoltTable table = response.getResults()[0];
		        while (table.advanceRow()) {
		        	this.writer.write(table.getString(0), table.getString(1), Long.toString(table.getLong(2)));
		        }
			}
		} catch (IOException | ProcCallException e) {
			e.printStackTrace();
		}
		
	}
	
	@Override
	public void close() {
		writer.close();
		
		try {
			client.close();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
	}
	
}
