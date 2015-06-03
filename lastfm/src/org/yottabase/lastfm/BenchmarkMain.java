package org.yottabase.lastfm;

import java.io.IOException;
import java.util.Properties;

import org.yottabase.lastfm.core.Config;
import org.yottabase.lastfm.core.Driver;
import org.yottabase.lastfm.core.DriverManager;
import org.yottabase.lastfm.importer.LineReader;
import org.yottabase.lastfm.importer.ListenedTrack;
import org.yottabase.lastfm.importer.ListenedTrackRecordManager;
import org.yottabase.lastfm.importer.SimpleLineReader;
import org.yottabase.lastfm.importer.User;
import org.yottabase.lastfm.importer.UserRecordManager;

public class BenchmarkMain {

	private static final String SEPARATOR = "\t";
	
	public static void main(String[] args) throws IOException {

		long startTime;
		
		Config config = new Config();
		Properties properties = config.getProperties();
		DriverManager driverManager = new DriverManager(properties);
		
		for (Driver driver : driverManager.getDrivers()) {

			System.out.println("Driver" + SEPARATOR + "Method" + SEPARATOR + "Time (ms)");
			
			//initializeSchema
			startTime = System.currentTimeMillis();
			driver.initializeSchema();
			System.out.println(driver.getClass().getSimpleName() + SEPARATOR + "initializeSchema" + SEPARATOR + (System.currentTimeMillis() - startTime));
			
			//importUserDataset
			startTime = System.currentTimeMillis();
			importUserDataset(properties.getProperty("dataset.user.filepath"), driver);
			System.out.println(driver.getClass().getSimpleName() + SEPARATOR + "importUserDataset" + SEPARATOR + (System.currentTimeMillis() - startTime));
			
			//importListenedTrackDataset
			startTime = System.currentTimeMillis();
			importListenedTrackDataset(properties.getProperty("dataset.listened_tracks.filepath"), driver);
			System.out.println(driver.getClass().getSimpleName() + SEPARATOR + "importListenedTrackDataset" + SEPARATOR + (System.currentTimeMillis() - startTime));
		}
	}	

	public static void importUserDataset(String filepath, Driver driver) {
		
		LineReader reader = new SimpleLineReader(filepath);
		UserRecordManager recordManager = new UserRecordManager();

		reader.getNextLine();
		
		String line;
		User user;

		while ((line = reader.getNextLine()) != null) {

			user = recordManager.getUserFromLine(line);

			driver.insertUser(user);
		}

	}
	
	public static void importListenedTrackDataset(String filepath, Driver driver){
		LineReader reader = new SimpleLineReader(filepath);
		ListenedTrackRecordManager recordManager = new ListenedTrackRecordManager();

		reader.getNextLine();
		
		String line;
		ListenedTrack listenedTrack;

		while ((line = reader.getNextLine()) != null) {

			listenedTrack = recordManager.getListenedTrackFromLine(line);

			driver.insertListenedTrack(listenedTrack);
		}
	}

}
