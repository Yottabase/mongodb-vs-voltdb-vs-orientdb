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
		String procedureFilename = properties.get("voltdb.procedure_filename");
		boolean asyncMode = properties.get("voltdb.async_mode").equals("true");
		
		try {
			
			Client client = ClientFactory.createClient(new ClientConfig(username, password));
			client.createConnection(host);
			
			if(procedureFilename == null){
				if(asyncMode)
					facade = new VoltDBAdapterAsyncInsert(client);
				else
					facade = new VoltDBAdapter(client);	
			}else{
				if(asyncMode)
					facade = new VoltDBAdapterAsyncInsert(client, procedureFilename);
				else
					facade = new VoltDBAdapter(client, procedureFilename);
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return facade;
		
	}

}
