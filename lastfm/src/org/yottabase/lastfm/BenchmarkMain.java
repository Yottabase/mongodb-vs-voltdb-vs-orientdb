package org.yottabase.lastfm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.yottabase.lastfm.core.Config;
import org.yottabase.lastfm.core.Driver;
import org.yottabase.lastfm.core.DriverFactory;
import org.yottabase.lastfm.importer.LineReader;
import org.yottabase.lastfm.importer.ListenedTrack;
import org.yottabase.lastfm.importer.ListenedTrackRecordManager;
import org.yottabase.lastfm.importer.SimpleLineReader;
import org.yottabase.lastfm.importer.User;
import org.yottabase.lastfm.importer.UserRecordManager;

public class BenchmarkMain {

	public static void main(String[] args) throws IOException {

		long startTime;
		
		Config config = new Config();
		Properties properties = config.getProperties();

		List<Driver> drivers = initDrivers(properties);

		
		
		for (Driver driver : drivers) {

			startTime = System.currentTimeMillis();
			driver.initializeSchema();
			System.out.println(driver.getClass().getSimpleName() + "_initializeSchema: " + (System.currentTimeMillis() - startTime) + " ms");
			
			startTime = System.currentTimeMillis();
			importUserDataset(properties.getProperty("dataset.user.filepath"), driver);
			System.out.println(driver.getClass().getSimpleName() + "_importUserDataset: " + (System.currentTimeMillis() - startTime) + " ms");
			
			startTime = System.currentTimeMillis();
			importListenedTrackDataset(properties.getProperty("dataset.listened_tracks.filepath"), driver);
			System.out.println(driver.getClass().getSimpleName() + "_importListenedTrackDataset: " + (System.currentTimeMillis() - startTime) + " ms");
		}
	}

	public static List<Driver> initDrivers(Properties properties)
			throws IOException {
		String[] driversName = { "noop", "voltdb", "mongodb", "orientdb" };

		List<Driver> drivers = new ArrayList<Driver>();

		for (String driverName : driversName) {

			String enabledFlagKey = driverName + ".enabled";
			String driverFactoryKey = driverName + ".factory";

			if (properties.get(enabledFlagKey).equals("true")) {

				String factoryClassName = properties
						.getProperty(driverFactoryKey);

				try {
					DriverFactory driverFactory = (DriverFactory) Class
							.forName(factoryClassName).newInstance();
					Driver driver = driverFactory.createService(properties);
					drivers.add(driver);

				} catch (InstantiationException | IllegalAccessException
						| ClassNotFoundException e) {
					e.printStackTrace();
				}
			}
		}

		return drivers;
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
