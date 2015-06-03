package org.yottabase.lastfm.driver.voltdb;

import java.io.IOException;
import java.util.Properties;

import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.yottabase.lastfm.core.Driver;
import org.yottabase.lastfm.core.DriverFactory;

public class VoltDbDriverFactory implements DriverFactory{

	@Override
	public Driver createService(Properties properties) {
		
		Driver driver = null;
		
		String host = properties.getProperty("voltdb.host");
		String username = properties.getProperty("voltdb.username");
		String password = properties.getProperty("voltdb.password");
			
		try {
			
			Client client = ClientFactory.createClient(new ClientConfig(username, password));
			client.createConnection(host);
			
			driver = new VoltDbDriver(client);
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return driver;
		
	}

}
