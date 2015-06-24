package org.yottabase.lastfm;

import java.io.IOException;
import org.yottabase.lastfm.core.AbstractDBFacade;
import org.yottabase.lastfm.core.DBAdapterManager;
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
		
		DBAdapterManager adapterManager = new DBAdapterManager(config);
		
		OutputWriterFactory outputWriterFactory = new OutputWriterFactory();
		
		Timer globalElapsedTime = new Timer("Total", true);
		Timer adapterElapsedTime;
		Timer methodElapsedTime;
		
		for (AbstractDBFacade dbAdapter : adapterManager.getAdapters()) {
			String adapterName = dbAdapter.getClass().getSimpleName();
			
			adapterElapsedTime = new Timer(adapterName, true);
			
			OutputWriter writer = outputWriterFactory.createService(config, dbAdapter.getClass().getSimpleName() + "_output.txt");
			dbAdapter.setWriter(writer);
			
			
			//BENCHMARK
			methodElapsedTime = new Timer(adapterName + SEPARATOR + "initializeSchema()", true);
			dbAdapter.initializeSchema();
			methodElapsedTime.pauseAndPrint();
			
			
			
			methodElapsedTime = new Timer(adapterName + SEPARATOR + "insertUser()", true);
			importUserDataset(config.get("dataset.user.filepath"), dbAdapter);
			methodElapsedTime.pauseAndPrint();
			
			
			
			methodElapsedTime = new Timer(adapterName + SEPARATOR + "insertListenedTrack()", true);
			importListenedTrackDataset(config.get("dataset.listened_tracks.filepath"), dbAdapter);
			methodElapsedTime.pauseAndPrint();
			
			
			
			methodElapsedTime = new Timer(adapterName + SEPARATOR + "countArtists()", true);
			dbAdapter.countArtists();
			methodElapsedTime.pauseAndPrint();
			
			
			
			methodElapsedTime = new Timer(adapterName + SEPARATOR + "countTracks()", true);
			dbAdapter.countTracks();
			methodElapsedTime.pauseAndPrint();
			
			
			
			methodElapsedTime = new Timer(adapterName + SEPARATOR + "countUsers()", true);
			dbAdapter.countUsers();
			methodElapsedTime.pauseAndPrint();
			
			
			
			methodElapsedTime = new Timer(adapterName + SEPARATOR + "countEntities()", true);
			dbAdapter.countEntities();
			methodElapsedTime.pauseAndPrint();
			
			
			
			methodElapsedTime = new Timer(adapterName + SEPARATOR + "averageNumberListenedTracksPerUser(true)", true);
			dbAdapter.averageNumberListenedTracksPerUser(true);
			methodElapsedTime.pauseAndPrint();
			
			methodElapsedTime = new Timer(adapterName + SEPARATOR + "averageNumberListenedTracksPerUser(false)", true);
			dbAdapter.averageNumberListenedTracksPerUser(false);
			methodElapsedTime.pauseAndPrint();
			
			
			
			methodElapsedTime = new Timer(adapterName + SEPARATOR + "averageNumberSungTracksPerArtist()", true);
			dbAdapter.averageNumberSungTracksPerArtist();
			methodElapsedTime.pauseAndPrint();
			
			
			
			//usersChart
			methodElapsedTime = new Timer(adapterName + SEPARATOR + "usersChart(n, true, true)", true);
			dbAdapter.usersChart(n, true, true);
			methodElapsedTime.pauseAndPrint();
			
			methodElapsedTime = new Timer(adapterName + SEPARATOR + "usersChart(n, false, true)", true);
			dbAdapter.usersChart(n, false, true);
			methodElapsedTime.pauseAndPrint();			

			methodElapsedTime = new Timer(adapterName + SEPARATOR + "usersChart(n, true, false)", true);
			dbAdapter.usersChart(n, true, false);
			methodElapsedTime.pauseAndPrint();			
			
			methodElapsedTime = new Timer(adapterName + SEPARATOR + "usersChart(n, false, false)", true);
			dbAdapter.usersChart(n, false, false);
			methodElapsedTime.pauseAndPrint();
			
			
			
			//tracksChart
			methodElapsedTime = new Timer(adapterName + SEPARATOR + "tracksChart(n, true, true)", true);
			dbAdapter.tracksChart(n, true, true);
			methodElapsedTime.pauseAndPrint();
			
			methodElapsedTime = new Timer(adapterName + SEPARATOR + "tracksChart(n, false, true)", true);
			dbAdapter.tracksChart(n, false, true);
			methodElapsedTime.pauseAndPrint();			

			methodElapsedTime = new Timer(adapterName + SEPARATOR + "tracksChart(n, true, false)", true);
			dbAdapter.tracksChart(n, true, false);
			methodElapsedTime.pauseAndPrint();			
			
			methodElapsedTime = new Timer(adapterName + SEPARATOR + "tracksChart(n, false, false)", true);
			dbAdapter.tracksChart(n, false, false);
			methodElapsedTime.pauseAndPrint();			

			
			
			//artistsChart
			methodElapsedTime = new Timer(adapterName + SEPARATOR + "artistsChart(n, true, true)", true);
			dbAdapter.artistsChart(n, true, true);
			methodElapsedTime.pauseAndPrint();
			
			methodElapsedTime = new Timer(adapterName + SEPARATOR + "artistsChart(n, false, true)", true);
			dbAdapter.artistsChart(n, false, true);
			methodElapsedTime.pauseAndPrint();			

			methodElapsedTime = new Timer(adapterName + SEPARATOR + "artistsChart(n, true, false)", true);
			dbAdapter.artistsChart(n, true, false);
			methodElapsedTime.pauseAndPrint();			
			
			methodElapsedTime = new Timer(adapterName + SEPARATOR + "artistsChart(n, false, false)", true);
			dbAdapter.artistsChart(n, false, false);
			methodElapsedTime.pauseAndPrint();						

			
			
			//tracksListenedTogether
			methodElapsedTime = new Timer(adapterName + SEPARATOR + "tracksListenedTogether(n)", true);
			dbAdapter.tracksListenedTogether(n);
			methodElapsedTime.pauseAndPrint();
			
			adapterElapsedTime.pauseAndPrint();
			writer.close();
		}
		
		globalElapsedTime.pauseAndPrint();
	}	

	public static void importUserDataset(String filepath, AbstractDBFacade dbAdapter) {
		
		LineReader reader = new SimpleLineReader(filepath);
		UserRecordManager recordManager = new UserRecordManager();

		reader.getNextLine();
		
		String line;
		User user;

		while ((line = reader.getNextLine()) != null) {

			user = recordManager.getUserFromLine(line);

			dbAdapter.insertUser(user);
		}

	}
	
	public static void importListenedTrackDataset(String filepath, AbstractDBFacade dbAdapter){
		LineReader reader = new SimpleLineReader(filepath);
		ListenedTrackRecordManager recordManager = new ListenedTrackRecordManager();

		reader.getNextLine();
		
		String line;
		ListenedTrack listenedTrack;

		while ((line = reader.getNextLine()) != null) {

			listenedTrack = recordManager.getListenedTrackFromLine(line);

			dbAdapter.insertListenedTrack(listenedTrack);
		}
	}

}
