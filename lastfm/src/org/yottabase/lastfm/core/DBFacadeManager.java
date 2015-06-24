package org.yottabase.lastfm.core;

import java.util.ArrayList;
import java.util.List;

public class DBFacadeManager {
	
	private PropertyFile properties;
	
	public DBFacadeManager(PropertyFile properties) {
		super();
		this.properties = properties;
	}

	public PropertyFile getProperties() {
		return properties;
	}

	public void setProperties(PropertyFile properties) {
		this.properties = properties;
	}

	public List<DBFacade> getFacades(){
		
		String[] facadesName = properties.get("supported_facades").split(",");

		List<DBFacade> facades = new ArrayList<DBFacade>();

		for (String facadeName : facadesName) {
			String enabledFlagKey = facadeName + ".enabled";
		
			if (properties.get(enabledFlagKey).equals("true")) {
				facades.add(this.getFacade(facadeName));
			}
		}

		return facades;
	}
	
	public DBFacade getFacade(String facadeName){
		DBFacade facade = null;
		
		
		String facadeFactoryKey = facadeName + ".factory";

		String factoryClassName = properties.get(facadeFactoryKey);

		try {
			DBFacadeFactory facadeFactory = (DBFacadeFactory) Class
					.forName(factoryClassName).newInstance();
			facade = facadeFactory.createService(properties);

		} catch (InstantiationException | IllegalAccessException
				| ClassNotFoundException e) {
			e.printStackTrace();
		}
		
		return facade;
	}

}
