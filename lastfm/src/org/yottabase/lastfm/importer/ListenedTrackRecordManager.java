package org.yottabase.lastfm.importer;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.List;

public class ListenedTrackRecordManager {

	public ListenedTrack getListenedTrackFromLine(String line) {

		SimpleDateFormat formatter = new SimpleDateFormat(
				"yyyy-MM-dd'T'HH:mm:ss'Z'");

		List<String> values = Arrays.asList(line.split("\t"));
		Iterator<String> iter = values.iterator();

		String code = (iter.hasNext()) ? iter.next() : "";
		String time = (iter.hasNext()) ? iter.next() : "";
		String artistCode = (iter.hasNext()) ? iter.next() : "";
		String artistName = (iter.hasNext()) ? iter.next() : "";
		String trackCode = (iter.hasNext()) ? iter.next() : "";
		String trackName = (iter.hasNext()) ? iter.next() : "";

		ListenedTrack listenedTrack = new ListenedTrack();
		listenedTrack.setCode(code);
		listenedTrack.setArtistCode((artistCode.equals("")) ? Encryption
				.toSHA1(artistName) : artistCode);
		listenedTrack
				.setArtistName((artistName.equals("")) ? null : artistName);
		listenedTrack.setTrackCode((trackCode.equals("")) ? Encryption
				.toSHA1(trackName) : trackCode);
		listenedTrack.setTrackName((trackName.equals("")) ? null : trackName);

		if (!time.equals("")) {
			try {
				Calendar calendar = new GregorianCalendar();
				calendar.setTime(formatter.parse(time));
				listenedTrack.setTime(calendar);
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}

		return listenedTrack;
	}

}
