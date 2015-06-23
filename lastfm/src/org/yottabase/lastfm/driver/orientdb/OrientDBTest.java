package org.yottabase.lastfm.driver.orientdb;

import java.io.IOException;
import java.util.Properties;

import org.yottabase.lastfm.core.Config;
import org.yottabase.lastfm.core.Facade;
import org.yottabase.lastfm.core.FacadeFactory;

public class OrientDBTest {
	
	public static void main(String[] args) throws IOException {
		Config config = new Config();
		Properties properties = config.getProperties();
		FacadeFactory factory = new OrientDBFacadeFactory();
		Facade orientdbFacade = factory.createService(properties);
		
		// counts
		orientdbFacade.countEntities();
		orientdbFacade.countArtists();
		orientdbFacade.countTracks();
		orientdbFacade.countUsers();
		
		// avarages
		orientdbFacade.averageNumberListenedTracksPerUser(false);
		orientdbFacade.averageNumberSungTracksPerArtist(false);
		
		// charts (top/last K)
		orientdbFacade.usersChart(1, true, false);		// NO DISTINCT
		orientdbFacade.tracksChart(1, true, false);
		orientdbFacade.artistsChart(1, true, false);
		
		// assocs
//		orientdbFacade.tracksListenedTogether(1);		// TO DO
		
	}

}
