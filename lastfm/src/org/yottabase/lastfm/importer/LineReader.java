package org.yottabase.lastfm.importer;

public interface LineReader {

	/**
	 * 
	 * @return
	 */
	public String getNextLine();

	/**
	 * 
	 */
	public void close();

}
