package org.yottabase.lastfm.core;

import java.util.ArrayList;
import java.util.List;

public class FacadeManager {
	
	private PropertyFile properties;
	
	public FacadeManager(PropertyFile properties) {
		super();
		this.properties = properties;
	}

	public PropertyFile getProperties() {
		return properties;
	}

	public void setProperties(PropertyFile properties) {
		this.properties = properties;
	}

	public List<Facade> getFacades(){
		
		String[] facadesName = properties.get("supported_facades").split(",");

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

		String factoryClassName = properties.get(facadeFactoryKey);

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
