package org.yottabase.lastfm.adapter.mongodb;

import java.util.Arrays;
import org.yottabase.lastfm.core.AbstractDBFacade;
import org.yottabase.lastfm.core.DBFacadeFactory;
import org.yottabase.lastfm.core.PropertyFile;

import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;

public class MongoDBAdapterFactory implements DBFacadeFactory {

	@Override
	public AbstractDBFacade createService(PropertyFile properties) {
		AbstractDBFacade driver = null;

		String host = properties.get("mongodb.host");
		String username = properties.get("mongodb.username");
		String password = properties.get("mongodb.password");

		MongoCredential credential = MongoCredential.createCredential(username,
				"lastfm", password.toCharArray());

		MongoClient mongoClient = new MongoClient(
				new ServerAddress(host, 27017), Arrays.asList(credential));

		driver = new MongoDBAdapter(mongoClient);

		return driver;
	}

}
