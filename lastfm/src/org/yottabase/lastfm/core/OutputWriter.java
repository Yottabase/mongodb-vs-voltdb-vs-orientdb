package org.yottabase.lastfm.core;

public interface OutputWriter {
	
	public void writeHeader(String... fields);
	
	public void write(String... fields);
	
	public void close();
}
