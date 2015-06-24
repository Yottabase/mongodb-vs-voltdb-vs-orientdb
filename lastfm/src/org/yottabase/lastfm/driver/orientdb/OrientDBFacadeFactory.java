package org.yottabase.lastfm.driver.orientdb;

import org.yottabase.lastfm.core.Facade;
import org.yottabase.lastfm.core.FacadeFactory;
import org.yottabase.lastfm.core.PropertyFile;

import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;

public class OrientDBFacadeFactory implements FacadeFactory {

	@Override
	public Facade createService(PropertyFile properties) {
		final String PREFIX = "orientdb.";

		// access properties
		String storage = properties.get(		PREFIX + "storage"		);
		String db_directory = properties.get(	PREFIX + "db_directory"	);
		String username = properties.get(		PREFIX + "username"		);
		String password = properties.get(		PREFIX + "password"		);
		
		// db as a graph
		OrientGraphFactory factory = new OrientGraphFactory(storage + ":" + db_directory, username, password);
		OrientGraph graph = factory.getTx();			// transactional instance
		
		// config & tuning
		graph.setUseLightweightEdges(true);					// do not use record for edges if unnecessary
		graph.setStandardElementConstraints(false);			// allows null values (and more..) for properties
		graph.getRawGraph().declareIntent(new OIntentMassiveInsert());
		
		Facade drvr = new OrientDBFacade(graph);
		return drvr;
	}

}
