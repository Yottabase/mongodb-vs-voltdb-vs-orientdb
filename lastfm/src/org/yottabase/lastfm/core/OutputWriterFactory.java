package org.yottabase.lastfm.core;

import java.util.Properties;

public class OutputWriterFactory {
	
	public OutputWriter createService(Properties properties, String filename){
		
		OutputWriter writer = null;
		
		if(properties.getProperty("output.type").equals("filesystem")){
			
			String filepath = properties.getProperty("output.folder") + "/" + filename;
			
			writer = new TSVFileOutputWriter(filepath);
			
		}else{
			
			writer = new ConsoleOutputWriter();
			
		}
		
		return writer;
		
	}

}
