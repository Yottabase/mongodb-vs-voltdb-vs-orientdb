package org.yottabase.lastfm.importer;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class GenericLineReader {
	
	private InputStream in;
	
	private BufferedReader reader;
	
	
	public GenericLineReader(String inputPath) {
		
		try {
			this.in = new FileInputStream( inputPath );
			this.reader = new BufferedReader( new InputStreamReader(in) );
		} 
		catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		
	}

	/**
	 * Restituisce il prossimo record presente nel file 
	 * di input corrente o null se non vi sono ulteriori record
	 */
	public String getNextRecord() {
		String record = null;
		
		try {
			record = reader.readLine();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return record;
	}

	/**
	 * Chiude lo stream ed il reader
	 */
	public void close() {
		try {
			this.in.close();
			this.reader.close();
		} 
		catch (IOException e) {
			e.printStackTrace();
		}
	}

}
