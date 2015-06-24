package org.yottabase.lastfm;

import java.io.IOException;
import org.yottabase.lastfm.core.DBFacade;
import org.yottabase.lastfm.core.DBFacadeManager;
import org.yottabase.lastfm.core.OutputWriter;
import org.yottabase.lastfm.core.OutputWriterFactory;
import org.yottabase.lastfm.core.PropertyFile;
import org.yottabase.lastfm.importer.LineReader;
import org.yottabase.lastfm.importer.ListenedTrack;
import org.yottabase.lastfm.importer.ListenedTrackRecordManager;
import org.yottabase.lastfm.importer.SimpleLineReader;
import org.yottabase.lastfm.importer.User;
import org.yottabase.lastfm.importer.UserRecordManager;

import utils.Timer;

public class BenchmarkMain {

	private static final String SEPARATOR = "\t";
	
	private static final String CONFIG_FILE_PATH = "config.properties";
	
	public static void main(String[] args) throws IOException {

		PropertyFile config;
		
		int n = 10;
		
		if(args.length > 0){
			config = new PropertyFile(args[0]);	
		}else{
			config = new PropertyFile(BenchmarkMain.class.getClassLoader().getResourceAsStream(CONFIG_FILE_PATH));
		}
		
		DBFacadeManager facadeManager = new DBFacadeManager(config);
		
		OutputWriterFactory outputWriterFactory = new OutputWriterFactory();
		
		Timer globalElapsedTime = new Timer("Total", true);
		Timer facadeElapsedTime;
		Timer methodElapsedTime;
		
		for (DBFacade facade : facadeManager.getFacades()) {
			String facadeName = facade.getClass().getSimpleName();
			
			facadeElapsedTime = new Timer(facadeName, true);
			
			OutputWriter writer = outputWriterFactory.createService(config, facade.getClass().getSimpleName() + "_output.txt");
			facade.setWriter(writer);
			
			
			//BENCHMARK
			methodElapsedTime = new Timer(facadeName + SEPARATOR + "initializeSchema()", true);
			facade.initializeSchema();
			methodElapsedTime.pauseAndPrint();
			
			
			
			methodElapsedTime = new Timer(facadeName + SEPARATOR + "insertUser()", true);
			importUserDataset(config.get("dataset.user.filepath"), facade);
			methodElapsedTime.pauseAndPrint();
			
			
			
			methodElapsedTime = new Timer(facadeName + SEPARATOR + "insertListenedTrack()", true);
			importListenedTrackDataset(config.get("dataset.listened_tracks.filepath"), facade);
			methodElapsedTime.pauseAndPrint();
			
			
			
			methodElapsedTime = new Timer(facadeName + SEPARATOR + "countArtists()", true);
			facade.countArtists();
			methodElapsedTime.pauseAndPrint();
			
			
			
			methodElapsedTime = new Timer(facadeName + SEPARATOR + "countTracks()", true);
			facade.countTracks();
			methodElapsedTime.pauseAndPrint();
			
			
			
			methodElapsedTime = new Timer(facadeName + SEPARATOR + "countUsers()", true);
			facade.countUsers();
			methodElapsedTime.pauseAndPrint();
			
			
			
			methodElapsedTime = new Timer(facadeName + SEPARATOR + "countEntities()", true);
			facade.countEntities();
			methodElapsedTime.pauseAndPrint();
			
			
			
			methodElapsedTime = new Timer(facadeName + SEPARATOR + "averageNumberListenedTracksPerUser(true)", true);
			facade.averageNumberListenedTracksPerUser(true);
			methodElapsedTime.pauseAndPrint();
			
			methodElapsedTime = new Timer(facadeName + SEPARATOR + "averageNumberListenedTracksPerUser(false)", true);
			facade.averageNumberListenedTracksPerUser(false);
			methodElapsedTime.pauseAndPrint();
			
			
			
			methodElapsedTime = new Timer(facadeName + SEPARATOR + "averageNumberSungTracksPerArtist()", true);
			facade.averageNumberSungTracksPerArtist();
			methodElapsedTime.pauseAndPrint();
			
			
			
			//usersChart
			methodElapsedTime = new Timer(facadeName + SEPARATOR + "usersChart(n, true, true)", true);
			facade.usersChart(n, true, true);
			methodElapsedTime.pauseAndPrint();
			
			methodElapsedTime = new Timer(facadeName + SEPARATOR + "usersChart(n, false, true)", true);
			facade.usersChart(n, false, true);
			methodElapsedTime.pauseAndPrint();			

			methodElapsedTime = new Timer(facadeName + SEPARATOR + "usersChart(n, true, false)", true);
			facade.usersChart(n, true, false);
			methodElapsedTime.pauseAndPrint();			
			
			methodElapsedTime = new Timer(facadeName + SEPARATOR + "usersChart(n, false, false)", true);
			facade.usersChart(n, false, false);
			methodElapsedTime.pauseAndPrint();
			
			
			
			//tracksChart
			methodElapsedTime = new Timer(facadeName + SEPARATOR + "tracksChart(n, true, true)", true);
			facade.tracksChart(n, true, true);
			methodElapsedTime.pauseAndPrint();
			
			methodElapsedTime = new Timer(facadeName + SEPARATOR + "tracksChart(n, false, true)", true);
			facade.tracksChart(n, false, true);
			methodElapsedTime.pauseAndPrint();			

			methodElapsedTime = new Timer(facadeName + SEPARATOR + "tracksChart(n, true, false)", true);
			facade.tracksChart(n, true, false);
			methodElapsedTime.pauseAndPrint();			
			
			methodElapsedTime = new Timer(facadeName + SEPARATOR + "tracksChart(n, false, false)", true);
			facade.tracksChart(n, false, false);
			methodElapsedTime.pauseAndPrint();			

			
			
			//artistsChart
			methodElapsedTime = new Timer(facadeName + SEPARATOR + "artistsChart(n, true, true)", true);
			facade.artistsChart(n, true, true);
			methodElapsedTime.pauseAndPrint();
			
			methodElapsedTime = new Timer(facadeName + SEPARATOR + "artistsChart(n, false, true)", true);
			facade.artistsChart(n, false, true);
			methodElapsedTime.pauseAndPrint();			

			methodElapsedTime = new Timer(facadeName + SEPARATOR + "artistsChart(n, true, false)", true);
			facade.artistsChart(n, true, false);
			methodElapsedTime.pauseAndPrint();			
			
			methodElapsedTime = new Timer(facadeName + SEPARATOR + "artistsChart(n, false, false)", true);
			facade.artistsChart(n, false, false);
			methodElapsedTime.pauseAndPrint();						

			
			
			//tracksListenedTogether
			methodElapsedTime = new Timer(facadeName + SEPARATOR + "tracksListenedTogether(n)", true);
			facade.tracksListenedTogether(n);
			methodElapsedTime.pauseAndPrint();
			
			facadeElapsedTime.pauseAndPrint();
			writer.close();
		}
		
		globalElapsedTime.pauseAndPrint();
	}	

	public static void importUserDataset(String filepath, DBFacade facade) {
		
		LineReader reader = new SimpleLineReader(filepath);
		UserRecordManager recordManager = new UserRecordManager();

		reader.getNextLine();
		
		String line;
		User user;

		while ((line = reader.getNextLine()) != null) {

			user = recordManager.getUserFromLine(line);

			facade.insertUser(user);
		}

	}
	
	public static void importListenedTrackDataset(String filepath, DBFacade facade){
		LineReader reader = new SimpleLineReader(filepath);
		ListenedTrackRecordManager recordManager = new ListenedTrackRecordManager();

		reader.getNextLine();
		
		String line;
		ListenedTrack listenedTrack;

		while ((line = reader.getNextLine()) != null) {

			listenedTrack = recordManager.getListenedTrackFromLine(line);

			facade.insertListenedTrack(listenedTrack);
		}
	}

}
