package org.yottabase.lastfm.driver.mongodb;

import java.util.Arrays;
import java.util.Properties;

import org.yottabase.lastfm.core.Facade;
import org.yottabase.lastfm.core.FacadeFactory;

import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;

public class MongodbFacadeFactory implements FacadeFactory {

	@Override
	public Facade createService(Properties properties) {
		Facade driver = null;

		String host = properties.getProperty("mongodb.host");
		String username = properties.getProperty("mongodb.username");
		String password = properties.getProperty("mongodb.password");

		MongoCredential credential = MongoCredential.createCredential(username,
				"lastfm", password.toCharArray());

		MongoClient mongoClient = new MongoClient(
				new ServerAddress(host, 27017), Arrays.asList(credential));

		driver = new MongodbFacade(mongoClient);

		return driver;
	}

}
