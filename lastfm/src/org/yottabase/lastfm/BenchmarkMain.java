package org.yottabase.lastfm;

import java.io.IOException;
import java.util.Properties;

import org.yottabase.lastfm.core.Config;
import org.yottabase.lastfm.core.Facade;
import org.yottabase.lastfm.core.FacadeManager;
import org.yottabase.lastfm.core.OutputWriter;
import org.yottabase.lastfm.core.OutputWriterFactory;
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
		FacadeManager facadeManager = new FacadeManager(properties);
		
		OutputWriterFactory outputWriterFactory = new OutputWriterFactory();
		
		System.out.println("Facade" + SEPARATOR + "Method" + SEPARATOR + "Time (ms)");
		
		for (Facade facade : facadeManager.getFacades()) {
			
			OutputWriter writer = outputWriterFactory.createService(properties, facade.getClass().getSimpleName() + "_output.txt");
			facade.setWriter(writer);
			
			//initializeSchema
			startTime = System.currentTimeMillis();
			facade.initializeSchema();
			System.out.println(facade.getClass().getSimpleName() + SEPARATOR + "initializeSchema" + SEPARATOR + (System.currentTimeMillis() - startTime));
			
			//importUserDataset
			startTime = System.currentTimeMillis();
//			importUserDataset(properties.getProperty("dataset.user.filepath"), facade);
			System.out.println(facade.getClass().getSimpleName() + SEPARATOR + "importUserDataset" + SEPARATOR + (System.currentTimeMillis() - startTime));
			
			//importListenedTrackDataset
			startTime = System.currentTimeMillis();
//			importListenedTrackDataset(properties.getProperty("dataset.listened_tracks.filepath"), facade);
			System.out.println(facade.getClass().getSimpleName() + SEPARATOR + "importListenedTrackDataset" + SEPARATOR + (System.currentTimeMillis() - startTime));
			
			//countArtists
			startTime = System.currentTimeMillis();
			facade.countArtists();
			System.out.println(facade.getClass().getSimpleName() + SEPARATOR + "countArtists" + SEPARATOR + (System.currentTimeMillis() - startTime));
			
			//countTracks
			startTime = System.currentTimeMillis();
			facade.countTracks();
			System.out.println(facade.getClass().getSimpleName() + SEPARATOR + "countTracks" + SEPARATOR + (System.currentTimeMillis() - startTime));
			
			//countUsers
			startTime = System.currentTimeMillis();
			facade.countUsers();
			System.out.println(facade.getClass().getSimpleName() + SEPARATOR + "countUsers" + SEPARATOR + (System.currentTimeMillis() - startTime));
			
			//countEntities
			startTime = System.currentTimeMillis();
			facade.countEntities();
			System.out.println(facade.getClass().getSimpleName() + SEPARATOR + "countEntities" + SEPARATOR + (System.currentTimeMillis() - startTime));
			
			//averageNumberListenedTracksPerUserUnique
			startTime = System.currentTimeMillis();
			facade.averageNumberListenedTracksPerUser(true);
			System.out.println(facade.getClass().getSimpleName() + SEPARATOR + "averageNumberListenedTracksPerUser" + SEPARATOR + (System.currentTimeMillis() - startTime));
			
			//averageNumberListenedTracksPerUserNotUnique
			startTime = System.currentTimeMillis();
			facade.averageNumberListenedTracksPerUser(false);
			System.out.println(facade.getClass().getSimpleName() + SEPARATOR + "averageNumberListenedTracksPerUserNotUnique" + SEPARATOR + (System.currentTimeMillis() - startTime));
			
			writer.close();
		}
	}	

	public static void importUserDataset(String filepath, Facade facade) {
		
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
	
	public static void importListenedTrackDataset(String filepath, Facade facade){
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
