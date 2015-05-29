package org.yottabase.lastfm.importer;

import java.util.StringTokenizer;

public class TSVLineParser implements LineParser {

	private final String DELIMITER = "\t";

	private StringTokenizer tokenizer;

	@Override
	public void parseLine(String line) {
		this.tokenizer = new StringTokenizer(line, DELIMITER);
	}

	@Override
	public String nextValue() {
		return this.tokenizer.nextToken();
	}

	@Override
	public boolean hasMoreValues() {
		return this.tokenizer.hasMoreTokens();
	}

}
