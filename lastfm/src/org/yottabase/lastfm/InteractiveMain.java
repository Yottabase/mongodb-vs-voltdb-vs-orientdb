package org.yottabase.lastfm;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import org.yottabase.lastfm.core.Facade;
import org.yottabase.lastfm.core.FacadeManager;
import org.yottabase.lastfm.core.OutputWriter;
import org.yottabase.lastfm.core.OutputWriterFactory;
import org.yottabase.lastfm.core.PropertyFile;

public class InteractiveMain {
	
	private static final String CONFIG_FILE_PATH = "config.properties";
	
	public static void main(String[] args) throws IOException, NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {

		args = new String[] {"noop", "countUsers", "10", "true", "true"};
		
		String facadeName = args[0]; 
		String methodName = args[1];
		String[] params = Arrays.copyOfRange(args, 2, args.length);
		
		PropertyFile config = new PropertyFile(CONFIG_FILE_PATH);
		
		FacadeManager facadeManager = new FacadeManager(config);
		Facade facade = facadeManager.getFacade(facadeName);
		
		OutputWriterFactory outputWriterFactory = new OutputWriterFactory();
		OutputWriter writer = outputWriterFactory.createService(config, facade.getClass().getSimpleName() + "_output.txt");
		facade.setWriter(writer);
		
		Method m = Facade.class.getMethod(methodName);
		
		long startTime = System.currentTimeMillis();
		
        m.invoke(facade); //TODO aggiungere parametri
        
        System.out.println((System.currentTimeMillis() - startTime));
					
		writer.close();
		
	}

}
