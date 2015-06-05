package org.yottabase.lastfm.driver.voltdb;

import java.io.IOException;
import java.util.Properties;

import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.yottabase.lastfm.core.Facade;
import org.yottabase.lastfm.core.FacadeFactory;

public class VoltDBFacadeFactory implements FacadeFactory{

	@Override
	public Facade createService(Properties properties) {
		
		Facade facade = null;
		
		String host = properties.getProperty("voltdb.host");
		String username = properties.getProperty("voltdb.username");
		String password = properties.getProperty("voltdb.password");
			
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
