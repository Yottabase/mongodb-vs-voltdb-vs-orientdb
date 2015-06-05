package org.yottabase.lastfm;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Properties;

import org.yottabase.lastfm.core.Config;
import org.yottabase.lastfm.core.Facade;
import org.yottabase.lastfm.core.FacadeManager;
import org.yottabase.lastfm.core.OutputWriter;
import org.yottabase.lastfm.core.OutputWriterFactory;

public class InteractiveMain {
	
	public static void main(String[] args) throws IOException, NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {

		args = new String[] {"noop", "countUsers", "10", "true", "true"};
		
		String facadeName = args[0]; 
		String methodName = args[1];
		String[] params = Arrays.copyOfRange(args, 2, args.length);
		
		Config config = new Config();
		Properties properties = config.getProperties();
		
		FacadeManager facadeManager = new FacadeManager(properties);
		Facade facade = facadeManager.getFacade(facadeName);
		
		OutputWriterFactory outputWriterFactory = new OutputWriterFactory();
		OutputWriter writer = outputWriterFactory.createService(properties, facade.getClass().getSimpleName() + "_output.txt");
		facade.setWriter(writer);
		
		Method m = Facade.class.getMethod(methodName);
		
		long startTime = System.currentTimeMillis();
		
        m.invoke(facade); //TODO aggiungere parametri
        
        System.out.println((System.currentTimeMillis() - startTime));
					
		writer.close();
		
	}

}
