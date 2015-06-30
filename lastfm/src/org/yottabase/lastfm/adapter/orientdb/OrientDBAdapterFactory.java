package org.yottabase.lastfm.adapter.orientdb;

import org.yottabase.lastfm.core.AbstractDBFacade;
import org.yottabase.lastfm.core.DBFacadeFactory;
import org.yottabase.lastfm.core.PropertyFile;

import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;

public class OrientDBAdapterFactory implements DBFacadeFactory {

	@Override
	public AbstractDBFacade createService(PropertyFile properties) {
		final String PREFIX = "orientdb.";

		// access properties
		String storage 		= properties.get(	PREFIX + "storage"		);
		String host 		= properties.get(	PREFIX + "host"			);
		String db_directory = properties.get(	PREFIX + "db_directory"	);
		String username 	= properties.get(	PREFIX + "username"		);
		String password 	= properties.get(	PREFIX + "password"		);
		String url = storage + ":" + (storage.equals("remote") ? host : "") + db_directory;
		
		// db as a graph
		OrientGraphFactory factory = new OrientGraphFactory(url, username, password);
		OrientGraph graph = factory.getTx();				// transactional instance
		
		// config & tuning
		graph.setUseLightweightEdges(true);					// do not use record for edges if unnecessary
		graph.setStandardElementConstraints(false);			// allows null values (and more..) for properties
		
		AbstractDBFacade drvr = new OrientDBAdapter(graph);
		return drvr;
	}

}
