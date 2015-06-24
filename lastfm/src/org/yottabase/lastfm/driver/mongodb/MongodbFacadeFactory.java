package org.yottabase.lastfm.driver.mongodb;

import java.util.Arrays;
import org.yottabase.lastfm.core.DBFacade;
import org.yottabase.lastfm.core.DBFacadeFactory;
import org.yottabase.lastfm.core.PropertyFile;

import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;

public class MongodbFacadeFactory implements DBFacadeFactory {

	@Override
	public DBFacade createService(PropertyFile properties) {
		DBFacade driver = null;

		String host = properties.get("mongodb.host");
		String username = properties.get("mongodb.username");
		String password = properties.get("mongodb.password");

		MongoCredential credential = MongoCredential.createCredential(username,
				"lastfm", password.toCharArray());

		MongoClient mongoClient = new MongoClient(
				new ServerAddress(host, 27017), Arrays.asList(credential));

		driver = new MongodbFacade(mongoClient);

		return driver;
	}

}
