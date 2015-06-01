package org.yottabase.lastfm.importer;

import java.util.Calendar;
import java.util.Date;

public class ListenedTrack {
	
	private String code;
	
	private Calendar time;
	
	private String artistCode;
	
	private String artistName;
	
	private String trackCode;
	
	private String trackName;

	public String getCode() {
		return code;
	}

	public void setCode(String code) {
		this.code = code;
	}

	public Calendar getTime() {
		return time;
	}
	
	public Date getTimeAsJavaDate() {
		return (time == null) ? null : time.getTime();
	}
	
	public long getTimeInMillis() {
		return (time == null) ? null : time.getTimeInMillis();
	}

	public void setTime(Calendar time) {
		this.time = time;
	}

	public String getArtistCode() {
		return artistCode;
	}

	public void setArtistCode(String artistCode) {
		this.artistCode = artistCode;
	}

	public String getArtistName() {
		return artistName;
	}

	public void setArtistName(String artistName) {
		this.artistName = artistName;
	}

	public String getTrackCode() {
		return trackCode;
	}

	public void setTrackCode(String trackCode) {
		this.trackCode = trackCode;
	}

	public String getTrackName() {
		return trackName;
	}

	public void setTrackName(String trackName) {
		this.trackName = trackName;
	}

	@Override
	public String toString() {
		return "ListenedTrack [code=" + code + ", artistName=" + artistName
				+ ", trackName=" + trackName + "]";
	}
	
}
