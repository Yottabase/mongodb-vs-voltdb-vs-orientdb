package org.yottabase.lastfm.driver.voltdb;

import java.io.File;
import java.io.IOException;

import org.voltdb.client.Client;
import org.voltdb.client.ProcCallException;
import org.yottabase.lastfm.core.Driver;
import org.yottabase.lastfm.driver.voltdb.procedure.InsertListenedTrackRecursive;
import org.yottabase.lastfm.importer.ListenedTrack;
import org.yottabase.lastfm.importer.User;

public class VoltDbDriver implements Driver{

	private static final String PROCEDURE_ADHOC = "@AdHoc";
	
	private final File procedureJar = new File("resources/voltdbProcedure.jar");
	
	private Client client;
	
	public VoltDbDriver(Client client) {
		this.client = client;
	}

	@Override
	public void initializeSchema() {
		
		String dqlClean = new StringBuilder()
		//elimina tabelle ed indici
		.append("DROP TABLE User IF EXISTS; ")
		.append("DROP INDEX User_Index IF EXISTS; ")
		.append("DROP TABLE Artist IF EXISTS; ")
		.append("DROP INDEX Artist_Index IF EXISTS; ")
		.append("DROP TABLE Track IF EXISTS; ")
		.append("DROP INDEX Track_Index IF EXISTS; ")
		.append("DROP TABLE ListenedTrack IF EXISTS; ")
		.append("DROP INDEX ListenedTrack_Index IF EXISTS; ")
		.append("DROP PROCEDURE " + InsertListenedTrackRecursive.class.getCanonicalName() + " IF EXISTS;")
		.toString();
		
		String dql = new StringBuilder()
			//crea tabella User
			.append("CREATE TABLE User ( ")
			.append("Code VARCHAR NOT NULL, Gender VARCHAR(1), Age TINYINT, Country VARCHAR, SignupDate TIMESTAMP,")
			.append("CONSTRAINT User_Index UNIQUE (Code), PRIMARY KEY (Code)")
			.append(");")
			
			//crea tabella Artist
			.append("CREATE TABLE Artist ( ")
			.append("Code VARCHAR NOT NULL, Name VARCHAR,")
			.append("CONSTRAINT Artist_Index UNIQUE (Code), PRIMARY KEY (Code)")
			.append(");")
			
			//crea tabella Track
			.append("CREATE TABLE Track ( ")
			.append("Code VARCHAR NOT NULL, Name VARCHAR, ArtistCode VARCHAR,")
			.append("CONSTRAINT Track_Index UNIQUE (Code), PRIMARY KEY (Code)")
			.append(");")
			
			//crea tabella ListenedTrack
			.append("CREATE TABLE ListenedTrack ( ")
			.append("Code VARCHAR NOT NULL, Time TIMESTAMP, TrackCode VARCHAR, UserCode VARCHAR,")
			.append("CONSTRAINT ListenedTrack_Index UNIQUE (Code), PRIMARY KEY (Code)")
			.append(");")
			
			.toString();
		
		String createProcedures = new StringBuilder()
			//crea procedura
			.append("CREATE PROCEDURE FROM CLASS " + InsertListenedTrackRecursive.class.getCanonicalName() + ";")
			
			.toString();
		
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
	
}
