package org.yottabase.lastfm.core;

public class ConsoleOutputWriter implements OutputWriter {

	@Override
	public void write(String... fields) {
		
		for(String field : fields) {
			System.out.print(field + "\t");
		}
		System.out.println();
		
	}

	@Override
	public void close() {
	}

}
