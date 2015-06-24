package org.yottabase.lastfm;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
//import java.util.Arrays;
import org.yottabase.lastfm.core.AbstractDBFacade;
import org.yottabase.lastfm.core.DBAdapterManager;
import org.yottabase.lastfm.core.OutputWriter;
import org.yottabase.lastfm.core.OutputWriterFactory;
import org.yottabase.lastfm.core.PropertyFile;

public class InteractiveMain {
	
	private static final String CONFIG_FILE_PATH = "config.properties";
	
	public static void main(String[] args) throws IOException, NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {

		args = new String[] {"noop", "countUsers", "10", "true", "true"};
		
		String adapterName = args[0]; 
		String methodName = args[1];
		//String[] params = Arrays.copyOfRange(args, 2, args.length);
		
		PropertyFile config = new PropertyFile(CONFIG_FILE_PATH);
		
		DBAdapterManager adapterManager = new DBAdapterManager(config);
		AbstractDBFacade adapter = adapterManager.getAdapter(adapterName);
		
		OutputWriterFactory outputWriterFactory = new OutputWriterFactory();
		OutputWriter writer = outputWriterFactory.createService(config, adapter.getClass().getSimpleName() + "_output.txt");
		adapter.setWriter(writer);
		
		Method m = AbstractDBFacade.class.getMethod(methodName);
		
		long startTime = System.currentTimeMillis();
		
        m.invoke(adapter); //TODO aggiungere parametri
        
        System.out.println((System.currentTimeMillis() - startTime));
					
		writer.close();
		
	}

}
