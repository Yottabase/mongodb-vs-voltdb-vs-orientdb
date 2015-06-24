package org.yottabase.lastfm.core;

public class OutputWriterFactory {

	public OutputWriter createService(PropertyFile properties, String filename) {

		OutputWriter writer = null;

		switch (properties.get("output.type")) {
			case "filesystem":
				String filepath = properties.get("output.folder") + "/" + filename;
				writer = new TSVFileOutputWriter(filepath);
				break;

			default:
				writer = new ConsoleOutputWriter();
		}

		return writer;

	}

}
