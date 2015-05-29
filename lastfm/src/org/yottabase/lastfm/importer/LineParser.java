package org.yottabase.lastfm.importer;

public interface LineParser {

	/**
	 * 
	 */
	public void parseLine(String line);

	/**
	 * 
	 * @return
	 */
	public String nextValue();

	/**
	 * 
	 * @return
	 */
	public boolean hasMoreValues();

}
