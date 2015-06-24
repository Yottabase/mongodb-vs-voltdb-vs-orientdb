package org.yottabase.lastfm.adapter.voltdb;

import java.io.IOException;

import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.yottabase.lastfm.core.AbstractDBFacade;
import org.yottabase.lastfm.core.DBFacadeFactory;
import org.yottabase.lastfm.core.PropertyFile;

public class VoltDBAdapterFactory implements DBFacadeFactory{

	@Override
	public AbstractDBFacade createService(PropertyFile properties) {
		
		AbstractDBFacade facade = null;
		
		String host = properties.get("voltdb.host");
		String username = properties.get("voltdb.username");
		String password = properties.get("voltdb.password");
			
		try {
			
			Client client = ClientFactory.createClient(new ClientConfig(username, password));
			client.createConnection(host);
			
			facade = new VoltDBAdapter(client);
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return facade;
		
	}

}
