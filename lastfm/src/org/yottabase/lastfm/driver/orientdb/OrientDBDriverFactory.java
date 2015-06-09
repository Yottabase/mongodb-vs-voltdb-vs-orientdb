package org.yottabase.lastfm.driver.orientdb;

import java.util.Properties;

import org.yottabase.lastfm.core.Driver;
import org.yottabase.lastfm.core.DriverFactory;

import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;

public class OrientDBDriverFactory implements DriverFactory {

	@Override
	public Driver createService(Properties properties) {
		final String PREFIX = "orientdb.";

		// access properties
		String storage = properties.getProperty(		PREFIX + "storage"		);
		String db_directory = properties.getProperty(	PREFIX + "db_directory"	);
		String username = properties.getProperty(		PREFIX + "username"		);
		String password = properties.getProperty(		PREFIX + "password"		);
		
		// db as a graph
		OrientGraphFactory factory = new OrientGraphFactory(storage + ":" + db_directory, username, password);
		OrientGraph graph = factory.getTx();			// transactional instance
		
		// config & tuning
		graph.setUseLightweightEdges(true);					// do not use record for edges if unnecessary
		graph.setStandardElementConstraints(false);			// allows null values (and more..) for properties
		graph.getRawGraph().declareIntent(new OIntentMassiveInsert());
		
		Driver drvr = new OrientDBDriver(graph);
		return drvr;
	}

}
