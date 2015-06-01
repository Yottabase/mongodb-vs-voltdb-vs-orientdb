package org.yottabase.lastfm.driver.voltdb;

import java.io.IOException;
import org.voltdb.client.Client;
import org.voltdb.client.ProcCallException;
import org.yottabase.lastfm.core.Driver;
import org.yottabase.lastfm.importer.ListenedTrack;
import org.yottabase.lastfm.importer.User;

public class VoltDbDriver implements Driver{

	private static final String PROCEDURE_ADHOC = "@AdHoc";
	
	private Client client;
	
	public VoltDbDriver(Client client) {
		this.client = client;
	}

	@Override
	public void initializeSchema() {
		
		String dqlClean = new StringBuilder()
		//elimina tabella User
		.append("DROP TABLE User IF EXISTS; ")
		.append("DROP INDEX User_Index IF EXISTS; ")
		.toString();
		
		String dql = new StringBuilder()
			//crea tabella User
			.append("CREATE TABLE User ( ")
			.append("Code VARCHAR NOT NULL, Gender VARCHAR(1), Age TINYINT, Country VARCHAR, SignupDate TIMESTAMP,")
			.append("CONSTRAINT User_Index UNIQUE (Code), PRIMARY KEY (Code)")
			.append(");")
			.toString();
		
		try {
			
			this.client.callProcedure( PROCEDURE_ADHOC,	dqlClean);
			this.client.callProcedure( PROCEDURE_ADHOC,	dql);
			
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
				( user.getSignupDate() != null ) ? user.getSignupDate().getTime() : null 	
			);
		} catch (IOException | ProcCallException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void insertListenedTrack(ListenedTrack listenedTrack) {
		// TODO Auto-generated method stub
		
	}

}
