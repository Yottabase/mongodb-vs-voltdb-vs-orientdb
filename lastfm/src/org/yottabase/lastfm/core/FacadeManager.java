package org.yottabase.lastfm.core;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class FacadeManager {
	
	private Properties properties;
	
	public FacadeManager(Properties properties) {
		super();
		this.properties = properties;
	}

	public Properties getProperties() {
		return properties;
	}

	public void setProperties(Properties properties) {
		this.properties = properties;
	}

	public List<Facade> getFacades(){
		
		String[] facadesName = properties.getProperty("supported_facades").split(",");

		List<Facade> facades = new ArrayList<Facade>();

		for (String facadeName : facadesName) {
			String enabledFlagKey = facadeName + ".enabled";
		
			if (properties.get(enabledFlagKey).equals("true")) {
				facades.add(this.getFacade(facadeName));
			}
		}

		return facades;
	}
	
	public Facade getFacade(String facadeName){
		Facade facade = null;
		
		
		String facadeFactoryKey = facadeName + ".factory";

		String factoryClassName = properties
				.getProperty(facadeFactoryKey);

		try {
			FacadeFactory facadeFactory = (FacadeFactory) Class
					.forName(factoryClassName).newInstance();
			facade = facadeFactory.createService(properties);

		} catch (InstantiationException | IllegalAccessException
				| ClassNotFoundException e) {
			e.printStackTrace();
		}
		
		return facade;
	}

}
