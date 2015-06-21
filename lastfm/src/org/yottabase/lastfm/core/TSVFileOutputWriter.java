package org.yottabase.lastfm.core;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;

public class TSVFileOutputWriter implements OutputWriter {

	PrintWriter writer = null;
	
	public TSVFileOutputWriter(String filename) {
		
		try {
			this.writer = new PrintWriter(new File(filename)); 
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		
	}

	@Override
	public void write(String... fields) {
		
		for(String field : fields) {
			this.writer.print(field + "\t");
		}
		this.writer.println();
	}
	
	@Override
	public void close() {
		this.writer.close();
	}

}
