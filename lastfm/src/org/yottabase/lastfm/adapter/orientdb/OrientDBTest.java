package org.yottabase.lastfm.adapter.orientdb;

import java.io.IOException;
import org.yottabase.lastfm.core.AbstractDBFacade;
import org.yottabase.lastfm.core.DBFacadeFactory;
import org.yottabase.lastfm.core.PropertyFile;

public class OrientDBTest {
	
	private static final String CONFIG_FILE_PATH = "/home/hduser/git/lastfm/lastfm/src/config.properties";
	
	public static void main(String[] args) throws IOException {
		PropertyFile config = new PropertyFile(CONFIG_FILE_PATH);
		DBFacadeFactory factory = new OrientDBAdapterFactory();
		AbstractDBFacade orientdbAdapter = factory.createService(config);
		
		// counts
//		orientdbAdapter.countEntities();
//		orientdbAdapter.countArtists();
//		orientdbAdapter.countTracks();
//		orientdbAdapter.countUsers();
//		
//		// avarages
//		orientdbAdapter.averageNumberListenedTracksPerUser(false);	// SQL
//		orientdbAdapter.averageNumberSungTracksPerArtist();			// SQL
//		
//		// charts (top/last K)
//		orientdbAdapter.usersChart(1, true, false);		// SQL - NO DISTINCT
//		orientdbAdapter.tracksChart(1, true, false);		// SQL
		orientdbAdapter.artistsChart(1, true, false);	// SQL
		
//		orientdbAdapter.artistByName("Coldplay");
//		orientdbAdapter.usersByAgeRange(10, 20);
//		orientdbAdapter.usersCountByCountry();
		
		orientdbAdapter.close();
		
	}

}
