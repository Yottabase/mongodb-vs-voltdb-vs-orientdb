package org.yottabase.lastfm.core;

import java.util.Properties;

public class OutputWriterFactory {

	public OutputWriter createService(Properties properties, String filename) {

		OutputWriter writer = null;

		switch (properties.getProperty("output.type")) {
			case "filesystem":
				String filepath = properties.getProperty("output.folder") + "/" + filename;
				writer = new TSVFileOutputWriter(filepath);
				break;

			default:
				writer = new ConsoleOutputWriter();
		}

		return writer;

	}

}
