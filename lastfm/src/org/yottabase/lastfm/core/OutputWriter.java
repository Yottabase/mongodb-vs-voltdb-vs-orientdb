package org.yottabase.lastfm.core;

public interface OutputWriter {
	
	public void write(String... fields);
	
	public void close();
}
