package org.yottabase.lastfm.driver.voltdb;

import java.io.IOException;

import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.yottabase.lastfm.core.Facade;
import org.yottabase.lastfm.core.FacadeFactory;
import org.yottabase.lastfm.core.PropertyFile;

public class VoltDBFacadeFactory implements FacadeFactory{

	@Override
	public Facade createService(PropertyFile properties) {
		
		Facade facade = null;
		
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
