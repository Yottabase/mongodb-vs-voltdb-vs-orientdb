package org.yottabase.lastfm.driver.voltdb;

import java.io.IOException;

import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.yottabase.lastfm.core.DBFacade;
import org.yottabase.lastfm.core.DBFacadeFactory;
import org.yottabase.lastfm.core.PropertyFile;

public class VoltDBFacadeFactory implements DBFacadeFactory{

	@Override
	public DBFacade createService(PropertyFile properties) {
		
		DBFacade facade = null;
		
		String host = properties.get("voltdb.host");
		String username = properties.get("voltdb.username");
		String password = properties.get("voltdb.password");
			
		try {
			
			Client client = ClientFactory.createClient(new ClientConfig(username, password));
			client.createConnection(host);
			
			facade = new VoltDBFacade(client);
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return facade;
		
	}

}
