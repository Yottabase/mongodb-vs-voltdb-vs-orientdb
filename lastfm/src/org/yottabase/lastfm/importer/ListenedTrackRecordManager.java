package org.yottabase.lastfm.importer;

import java.util.Arrays;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.List;

public class ListenedTrackRecordManager {
	
	public ListenedTrack getListenedTrackFromLine(String line) {
		
		List<String> values = Arrays.asList( line.split("\t") );
		Iterator<String> iter = values.iterator();
		
		String code = (iter.hasNext()) ? iter.next() : "";
		String time = (iter.hasNext()) ? iter.next() : "";
		String artistCode = (iter.hasNext()) ? iter.next() : "";
		String artistName = (iter.hasNext()) ? iter.next() : "";
		String trackCode = (iter.hasNext()) ? iter.next() : "";
		String trackName = (iter.hasNext()) ? iter.next() : "";
		
		ListenedTrack listenedTrack = new ListenedTrack();
		listenedTrack.setCode(code);
		listenedTrack.setTime(new GregorianCalendar());
		listenedTrack.setArtistCode((artistCode.equals("")) ? null : artistCode );
		listenedTrack.setArtistName((artistName.equals("")) ? null : artistName );
		listenedTrack.setTrackCode((trackCode.equals("")) ? null : trackCode );
		listenedTrack.setTrackName((trackName.equals("")) ? null : trackName );
		
		return listenedTrack;
	}

}
