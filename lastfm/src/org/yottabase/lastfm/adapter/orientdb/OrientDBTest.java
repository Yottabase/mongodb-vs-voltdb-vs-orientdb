package org.yottabase.lastfm.adapter.orientdb;

import java.io.IOException;
import org.yottabase.lastfm.core.AbstractDBFacade;
import org.yottabase.lastfm.core.DBFacadeFactory;
import org.yottabase.lastfm.core.PropertyFile;

public class OrientDBTest {
	
	private static final String CONFIG_FILE_PATH = "config.properties";
	
	public static void main(String[] args) throws IOException {
		PropertyFile config = new PropertyFile(CONFIG_FILE_PATH);
		DBFacadeFactory factory = new OrientDBAdapterFactory();
		AbstractDBFacade orientdbFacade = factory.createService(config);
		
		// counts
		orientdbFacade.countEntities();
		orientdbFacade.countArtists();
		orientdbFacade.countTracks();
		orientdbFacade.countUsers();
		
		// avarages
		orientdbFacade.averageNumberListenedTracksPerUser(false);
		orientdbFacade.averageNumberSungTracksPerArtist();
		
		// charts (top/last K)
		orientdbFacade.usersChart(1, true, false);		// NO DISTINCT
		orientdbFacade.tracksChart(1, true, false);
		orientdbFacade.artistsChart(1, true, false);
		
		// assocs
//		orientdbFacade.tracksListenedTogether(1);		// TO DO
		
	}

}
